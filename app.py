import os
import json
from dotenv import load_dotenv
from neo4j import GraphDatabase
from openai import OpenAI
import gradio as gr
import re

# --- 1. Setup & Environment ---
load_dotenv()
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

client = OpenAI(api_key=OPENAI_API_KEY)
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# --- 2. Database Stats & Topic Logic ---

def get_enrichment_stats():
    query = """
    MATCH (v:Verse)
    WITH count(v) AS total
    CALL () { MATCH (v1:Verse) WHERE v1.translation IS NOT NULL RETURN count(v1) AS with_trans }
    CALL () { MATCH (v2:Verse) WHERE v2.word_by_word_native IS NOT NULL RETURN count(v2) AS with_wbw }
    CALL () { MATCH (v3:Verse)-[:DISCUSSES]->(:Topic) RETURN count(DISTINCT v3) AS with_topics }
    RETURN total, with_trans, with_wbw, with_topics
    """
    try:
        with driver.session() as session:
            record = session.run(query).single()
            if not record: return "📊 Database empty."
            total = record["total"] or 1
            trans, wbw, topics = record["with_trans"], record["with_wbw"], record["with_topics"]
            p_trans = round((trans / total) * 100, 2)
            p_wbw = round((wbw / total) * 100, 2)
            p_topics = round((topics / total) * 100, 2)
            
            return f"""
### 📊 Migration Progress
- **Total Verses:** {total:,}
---
- **Enriched (Trans):** {trans:,} ({p_trans}%)
- **Word-by-Word:** {wbw:,} ({p_wbw}%)
- **Linked Topics:** {topics:,} ({p_topics}%)

**Overall Completion:** {p_trans}%
            """
    except Exception as e: return f"⚠️ Stats Error: {str(e)}"

def get_verses_for_topic(evt: gr.SelectData):
    # evt.value will be the Topic Name from the first column
    topic_name = evt.value if isinstance(evt.value, str) else evt.value[0]
    
    query = """
    MATCH (t:Topic {name: $tname})<-[:DISCUSSES]-(v:Verse)-[:PART_OF]->(s:Scripture)
    RETURN s.title AS scripture, v.relative_path AS verse, v.text AS text
    LIMIT 50
    """
    try:
        with driver.session() as session:
            result = session.run(query, tname=topic_name)
            details = []
            for r in result:
                details.append([r["scripture"], r["verse"], r["text"]])
            
            if not details:
                return f"### No verses found for: {topic_name}", []
            
            return f"### 📖 Verses discussing: {topic_name}", details
    except Exception as e:
        return f"⚠️ Error: {str(e)}", []

def get_topics_table(page=0, search_query=""):
    page_size = 25
    query = """
    MATCH (t:Topic)<-[r:DISCUSSES]-(:Verse)
    RETURN t.name AS name, count(r) AS verse_count
    """
    
    try:
        with driver.session() as session:
            result = session.run(query)
            aggregated_topics = {}
            
            # Cleaning patterns
            numbered_list_pattern = re.compile(r"^\d+\.\s*")
            bullet_pattern = re.compile(r"^[ \t]*[-*:]+[ \t]*")

            for record in result:
                name = record["name"]
                count = record["verse_count"]
                if not name: continue
                
                clean_name = re.sub(r"[\[\]\"']", "", name)
                parts = re.split(r',|\n', clean_name)
                
                for p in parts:
                    t = p.strip()
                    t = numbered_list_pattern.sub("", t)
                    t = bullet_pattern.sub("", t)
                    t = t.strip("*:- ")
                    
                    token = t.title()
                    # Apply search filter if provided
                    if search_query and search_query.lower() not in token.lower():
                        continue
                        
                    if token and len(token) > 1:
                        aggregated_topics[token] = aggregated_topics.get(token, 0) + count

            # Sort and Paginate
            sorted_topic_names = sorted(aggregated_topics.keys())
            start = page * page_size
            end = start + page_size
            page_items = sorted_topic_names[start:end]
            
            # Format as rows for gr.Dataframe
            table_data = []
            for name in page_items:
                table_data.append([name, aggregated_topics[name]])
            
            return table_data if table_data else [["No topics found", 0]]
            
    except Exception as e:
        return [[f"Error: {e}", 0]]

# --- 3. Original Perspectives & Chat Logic ---

def get_perspectives_from_graph(user_query):
    # This is your original function exactly as sent
    try:
        with driver.session() as session:
            s_result = session.run("MATCH (s:Scripture) RETURN s.name AS name")
            available_scriptures = [record["name"] for record in s_result]
            a_result = session.run("MATCH (a:Author) RETURN a.name AS name LIMIT 100")
            available_authors = [record["name"] for record in a_result]
    except Exception as e:
        print(f"Error fetching metadata: {e}")
        return [], {}

    extraction_prompt = f"""
    Identify entities in the user query.
    VALID SCRIPTURES: {available_scriptures}
    VALID AUTHORS: {available_authors}
    Question: "{user_query}"
    Return JSON: {{"scriptures": [], "locations": [], "topics": [], "authors": []}}
    """
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": extraction_prompt}],
        response_format={"type": "json_object"},
    )
    ents = json.loads(response.choices[0].message.content)
    params = {
        "scriptures": ents.get("scriptures", []),
        "authors": ents.get("authors", []),
        "topics": [t.strip().title() for t in ents.get("topics", [])],
        "locations": [l.strip().title() for l in ents.get("locations", [])],
    }

    cypher_query = """
    MATCH (v:Verse)-[:PART_OF]->(s:Scripture)
    OPTIONAL MATCH (auth:Author)-[:AUTHORED]->(v)
    OPTIONAL MATCH (v)-[:DISCUSSES]->(t:Topic)
    WITH v, s, auth, t,
         CASE WHEN size($topics) > 0 AND t.name IN $topics THEN 200 ELSE 0 END as t_score,
         CASE WHEN size($scriptures) > 0 AND s.name IN $scriptures THEN 100 ELSE 0 END as s_score,
         CASE WHEN size($authors) > 0 AND auth.name IN $authors THEN 150 ELSE 0 END as a_score
    WHERE (size($topics) = 0 OR t_score > 0)
      AND (size($scriptures) = 0 OR s_score > 0)
      AND (size($authors) = 0 OR a_score > 0)
    RETURN s.title AS scripture, v.relative_path AS verse_title, 
           v.text as verse_text, v.translation AS meaning, 
           v.word_by_word_native AS wbw,
           COALESCE(auth.name, v.author) as author,
           (t_score + s_score + a_score) as total_score
    ORDER BY total_score DESC
    LIMIT 15
    """
    context_data = []
    with driver.session() as session:
        result = session.run(cypher_query, **params)
        for record in result:
            raw_meaning = record["meaning"] or ""
            wbw_json = record["wbw"]
            formatted_wbw = ""
            if wbw_json:
                try:
                    wbw_list = json.loads(wbw_json)
                    if isinstance(wbw_list, list):
                        wbw_parts = [f"{item.get('word', '')}: {item.get('meaning', '')}" for item in wbw_list]
                        formatted_wbw = "\nWord-by-Word: " + " | ".join(wbw_parts)
                except Exception as e:
                    print(f"Error parsing WBW: {e}")

            context_data.append({
                "scripture": record["scripture"],
                "verse": record["verse_title"],
                "verse_text": record["verse_text"],
                "meaning": f"{raw_meaning}{formatted_wbw}",
            })
    return context_data, params

def bhashyam_chat(message, history):
    try:
        context, identified_topics = get_perspectives_from_graph(message)
        if not context:
            yield "🔍 No matching verses found."
            return

        formatted_context = ""
        for c in context:
            formatted_context += f"\nFROM [{c['scripture']}] ({c['verse']}): {c['verse_text']} : {c['meaning']}\n"

        system_prompt = f"You are the Bhashyam AI Research Assistant. Use 'CONTEXT FROM GRAPH' to answer. Cite [Scripture Name] and [Verse Title].\n### CONTEXT FROM GRAPH:\n{formatted_context}"
        stream = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": message}],
            stream=True,
        )
        partial_message = ""
        for chunk in stream:
            if chunk.choices[0].delta.content:
                partial_message += chunk.choices[0].delta.content
                yield partial_message
    except Exception as e:
        yield f"⚠️ Error: {str(e)}"

# --- 4. Tabbed UI ---

with gr.Blocks() as demo:
    gr.HTML("<h1 style='text-align: center;'>🕉️ Bhashyam AI - Research Suite</h1>")
    
    with gr.Tabs():
        with gr.Tab("💬 Research Chat"):
            with gr.Row():
                with gr.Column(scale=4):
                    chatbot = gr.Chatbot(height=600, show_label=False)
                    with gr.Row():
                        msg = gr.Textbox(placeholder="Ask your query...", label="Ask Bhashyam", scale=9)
                        submit_btn = gr.Button("Send", variant="primary", scale=1)
                with gr.Column(scale=1, variant="panel"):
                    stats_sidebar = gr.Markdown(get_enrichment_stats())
                    refresh_btn = gr.Button("🔄 Refresh Stats")

        with gr.Tab("🏷️ Topics Index"):
            current_page = gr.State(0)
            
            with gr.Row():
                with gr.Column(scale=2):
                    gr.Markdown("### 1. Select a Topic")
                    topic_search = gr.Textbox(placeholder="Filter topics...", label="Search Topics")
                    topics_table = gr.Dataframe(
                        headers=["Topic Name", "Verse Count"],
                        datatype=["str", "number"],
                        value=get_topics_table(0),
                        interactive=False
                    )
                    with gr.Row():
                        prev_btn = gr.Button("⬅️ Previous")
                        next_btn = gr.Button("Next ➡️")

                with gr.Column(scale=3):
                    detail_header = gr.Markdown("### 2. Topic Details\n*Click a row on the left to view verses.*")
                    verse_detail_table = gr.Dataframe(
                        headers=["Scripture", "Verse ID", "Text"],
                        datatype=["str", "str", "str"],
                        wrap=True, # Important for reading long verse texts
                        interactive=False
                    )

    # Original logic for chatbot history (tuples)
    def user_action(user_message, history):
        return "", history + [[user_message, None]]

    def bot_action(history):
        user_message = history[-1][0]
        bot_response = bhashyam_chat(user_message, history)
        history[-1][1] = ""
        for chunk in bot_response:
            history[-1][1] = chunk
            yield history

    # Event Bindings
    submit_btn.click(user_action, [msg, chatbot], [msg, chatbot]).then(bot_action, chatbot, chatbot)
    msg.submit(user_action, [msg, chatbot], [msg, chatbot]).then(bot_action, chatbot, chatbot)
    refresh_btn.click(get_enrichment_stats, outputs=stats_sidebar)
    
    # --- New Event Bindings for Table ---
    def move_page_table(page, delta, search):
        new_page = max(0, page + delta)
        return new_page, get_topics_table(new_page, search)

    def filter_topics(search):
        return 0, get_topics_table(0, search)

    next_btn.click(move_page_table, [current_page, gr.State(1), topic_search], [current_page, topics_table])
    prev_btn.click(move_page_table, [current_page, gr.State(-1), topic_search], [current_page, topics_table])
    topic_search.submit(filter_topics, inputs=topic_search, outputs=[current_page, topics_table])
    topics_table.select(
        fn=get_verses_for_topic,
        outputs=[detail_header, verse_detail_table]
    )    



if __name__ == "__main__":
    demo.queue().launch(theme=gr.themes.Default(primary_hue="orange", secondary_hue="gray"))