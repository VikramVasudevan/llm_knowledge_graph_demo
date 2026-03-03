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

# Global cache to keep the UI in sync with the Database
TOPIC_TO_NODES_MAP = {}

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
    global TOPIC_TO_NODES_MAP
    # Get the clean name the user clicked
    clean_topic_name = evt.value if isinstance(evt.value, str) else evt.value[0]
    
    # Retrieve the raw node names we mapped earlier
    raw_names = TOPIC_TO_NODES_MAP.get(clean_topic_name, [])
    
    if not raw_names:
        return f"### No raw mapping found for: {clean_topic_name}", []

    # Use 'IN' to match any of the original raw nodes
    query = """
    MATCH (t:Topic)<-[:DISCUSSES]-(v:Verse)-[:PART_OF]->(s:Scripture)
    WHERE t.name IN $raw_names
    RETURN s.title AS scripture, 
           v.relative_path AS verse, 
           v.text AS text, 
           v.translation AS translation
    LIMIT 5000
    """

    try:
        with driver.session() as session:
            result = session.run(query, raw_names=raw_names)
            details = []
            for r in result:
                details.append([
                    r["scripture"], 
                    r["verse"], 
                    r["text"], 
                    r["translation"] or "No translation available"
                ])
            
            if not details:
                return f"### No verses found for: {clean_topic_name}", []
            
            return f"### 📖 Verses discussing: {clean_topic_name}", details
    except Exception as e:
        return f"⚠️ Error: {str(e)}", []

def get_all_topics_table(search_query=""):
    global TOPIC_TO_NODES_MAP
    query = """
    MATCH (t:Topic)<-[r:DISCUSSES]-(:Verse)
    RETURN t.name AS name, count(r) AS verse_count
    """
    
    try:
        with driver.session() as session:
            result = session.run(query)
            aggregated_topics = {}
            TOPIC_TO_NODES_MAP = {} # Reset the map
            
            numbered_p = re.compile(r"^\d+\.\s*")
            bullet_p = re.compile(r"^[ \t]*[-*:]+[ \t]*")

            for record in result:
                raw_node_name = record["name"]
                count = record["verse_count"]
                if not raw_node_name: continue
                
                clean_name = re.sub(r"[\[\]\"']", "", raw_node_name)
                parts = re.split(r',|\n', clean_name)
                
                for p in parts:
                    t = p.strip()
                    t = numbered_p.sub("", t)
                    t = bullet_p.sub("", t)
                    t = t.strip("*:- ").title()
                    
                    if t and len(t) > 1:
                        # 1. Store the count
                        aggregated_topics[t] = aggregated_topics.get(t, 0) + count
                        
                        # 2. Map the clean name back to the RAW name for the detail query
                        if t not in TOPIC_TO_NODES_MAP:
                            TOPIC_TO_NODES_MAP[t] = []
                        if raw_node_name not in TOPIC_TO_NODES_MAP[t]:
                            TOPIC_TO_NODES_MAP[t].append(raw_node_name)

            # 3. Filter and Sort
            sorted_names = sorted(aggregated_topics.keys())
            if search_query:
                sorted_names = [n for n in sorted_names if search_query.lower() in n.lower()]
            
            return [[name, aggregated_topics[name]] for name in sorted_names]
            
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

        # Topics Tab
        with gr.Tab("🏷️ Topics Index"):
            with gr.Row():
                # --- Left Column: Search & Scrollable Table ---
                with gr.Column(scale=2):
                    gr.Markdown("### 🔍 Filter & Browse")
                    # Dataframe with a fixed height creates a scrollable area
                    topics_table = gr.Dataframe(
                        headers=["Topic Name", "Verse Count"],
                        datatype=["str", "number"],
                        value=get_all_topics_table(),
                        interactive=False,
                        show_search="search",
                        column_widths=[200,50]
                    )

                # --- Right Column: Verse Details ---
                with gr.Column(scale=4):
                    detail_header = gr.Markdown("### 📖 Topic Details\n*Select a topic on the left to view verses.*")
                    verse_detail_table = gr.Dataframe(
                        headers=["Scripture", "Verse ID", "Original Text", "English Translation"],
                        datatype=["str", "str", "str", "str"],
                        wrap=True,
                        interactive=False
                    )

    # Selecting a row updates the details
    topics_table.select(fn=get_verses_for_topic, outputs=[detail_header, verse_detail_table])

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
    
if __name__ == "__main__":
    demo.queue().launch(theme=gr.themes.Default(primary_hue="orange", secondary_hue="gray"))