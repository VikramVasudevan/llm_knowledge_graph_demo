import os
import json
from dotenv import load_dotenv
from neo4j import GraphDatabase
from openai import OpenAI
import gradio as gr

# --- 1. Setup & Environment ---
load_dotenv()
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

client = OpenAI(api_key=OPENAI_API_KEY)
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# --- 2. Database Stats Logic ---

def get_enrichment_stats():
    # Modern GQL Syntax: Added () to the CALL blocks to specify an empty scope
    query = """
    MATCH (v:Verse)
    WITH count(v) AS total
    CALL () { 
        MATCH (v1:Verse) WHERE v1.translation IS NOT NULL RETURN count(v1) AS with_trans 
    }
    CALL () { 
        MATCH (v2:Verse) WHERE v2.word_by_word_native IS NOT NULL RETURN count(v2) AS with_wbw 
    }
    CALL () { 
        MATCH (v3:Verse)-[:DISCUSSES]->(:Topic) RETURN count(DISTINCT v3) AS with_topics 
    }
    RETURN total, with_trans, with_wbw, with_topics
    """
    try:
        with driver.session() as session:
            record = session.run(query).single()
            if not record: return "📊 Database empty."
            
            total = record["total"] or 1
            trans = record["with_trans"] or 0
            wbw = record["with_wbw"] or 0
            topics = record["with_topics"] or 0

            # Calculate individual percentages
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
    except Exception as e:
        return f"⚠️ Stats Error: {str(e)}"

# --- 3. Graph Navigation Logic ---

def get_perspectives_from_graph(user_query):
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

    # Added v.word_by_word_native to the RETURN clause
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
            
            # --- Format Word-by-Word ---
            formatted_wbw = ""
            if wbw_json:
                try:
                    # Parse the JSON string from Neo4j
                    wbw_list = json.loads(wbw_json)
                    if isinstance(wbw_list, list):
                        # Create a string like "Word (Meaning), Word (Meaning)"
                        wbw_parts = [f"{item.get('word', '')}: {item.get('meaning', '')}" for item in wbw_list]
                        formatted_wbw = "\nWord-by-Word: " + " | ".join(wbw_parts)
                except Exception as e:
                    print(f"Error parsing WBW for {record['verse_title']}: {e}")

            context_data.append({
                "scripture": record["scripture"],
                "verse": record["verse_title"],
                "verse_text": record["verse_text"],
                # Append formatted WBW to the translation meaning
                "meaning": f"{raw_meaning}{formatted_wbw}",
            })
            
    return context_data, params

# --- 4. Chat Interface Logic ---

def bhashyam_chat(message, history):
    try:
        context, identified_topics = get_perspectives_from_graph(message)

        print("identified_topics = ", identified_topics)
        
        if not context:
            yield "🔍 No matching verses found in the graph for this query."
            return

        formatted_context = ""
        for c in context:
            formatted_context += f"\nFROM [{c['scripture']}] ({c['verse']}): {c['verse_text']} : {c['meaning']}\n"

        print("formatted_context = ", formatted_context)

        system_prompt = f"""
        You are the Bhashyam AI Research Assistant.
        Use 'CONTEXT FROM GRAPH' to answer. Cite [Scripture Name] and [Verse Title].
        ### CONTEXT FROM GRAPH:
        {formatted_context}
        """

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

# --- 5. Custom UI with Gradio Blocks ---

with gr.Blocks() as demo:
    gr.HTML("<h1 style='text-align: center;'>🕉️ Bhashyam AI - Research Suite</h1>")
    
    with gr.Row():
        # Sidebar
        with gr.Column(scale=1, variant="panel"):
            stats_output = gr.Markdown(get_enrichment_stats())
            refresh_btn = gr.Button("🔄 Refresh Stats", variant="secondary")
            gr.HTML("<hr>")
            gr.Markdown("### 🛠️ Config\n- **Model:** GPT-4o\n- **Database:** Neo4j Local")

        # Main Chat
        with gr.Column(scale=4):
            # FIXED: Removed 'type="tuples"' for older Gradio compatibility
            chatbot = gr.Chatbot(height=650, show_label=False)
            with gr.Row():
                msg = gr.Textbox(placeholder="Enter your philosophical query...", label="Ask Bhashyam", scale=9)
                submit_btn = gr.Button("Send", variant="primary", scale=1)

    # Event Bindings
    def user_action(user_message, history):
        # history is now a list of dicts: [{"role": "user", "content": "..."}, ...]
        new_history = history + [{"role": "user", "content": user_message}]
        return "", new_history

    def bot_action(history):
        user_message = history[-1]["content"]
        bot_response = bhashyam_chat(user_message, history)
        
        # Append an empty assistant message to the history
        history.append({"role": "assistant", "content": ""})
        
        for chunk in bot_response:
            # Update the last message (the assistant's response) with chunks
            history[-1]["content"] = chunk
            yield history

    # Bindings
    submit_btn.click(user_action, [msg, chatbot], [msg, chatbot]).then(
        bot_action, chatbot, chatbot
    )
    msg.submit(user_action, [msg, chatbot], [msg, chatbot]).then(
        bot_action, chatbot, chatbot
    )
    refresh_btn.click(fn=get_enrichment_stats, outputs=stats_output)

if __name__ == "__main__":
    demo.queue().launch(theme=gr.themes.Default(primary_hue="orange", secondary_hue="gray"))