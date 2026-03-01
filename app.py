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

# --- 2. Graph Navigation Logic ---

def get_perspectives_from_graph(user_query):
    # 1. Structured extraction (Same as yours)
    extraction_prompt = f"""
    Identify entities in the query. 
    Scriptures: {['Divya Prabandham', 'Srimad Bhagavatham', 'Bhagavad Gita']} (examples)
    Locations: Specific holy places or Divya Desams.
    Topics: Philosophical concepts.
    Authors: Poets or Rishis.

    Question: "{user_query}"
    Return JSON: {{"scriptures": [], "locations": [], "topics": [], "authors": []}}
    """
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": extraction_prompt}],
        response_format={ "type": "json_object" }
    )
    ents = json.loads(response.choices[0].message.content)
    
    params = {
        "scriptures": [s.strip() for s in ents.get('scriptures', [])],
        "locations": [l.strip().title() for l in ents.get('locations', [])],
        "topics": [t.strip().title() for t in ents.get('topics', [])],
        "authors": [a.strip().title() for a in ents.get('authors', [])]
    }

    # 2. Refined Cypher Query
    # Logic: If a parameter is provided, it MUST match. 
    # We also prioritize the specific scripture by adding an ORDER BY.
    cypher_query = """
        MATCH (v:Verse)-[:PART_OF]->(s:Scripture)
        OPTIONAL MATCH (auth:Author)-[:AUTHORED]->(v) 

        WITH v, s, auth,
             CASE 
                WHEN size($scriptures) > 0 AND ANY(x IN $scriptures WHERE toLower(s.title) CONTAINS toLower(x) OR toLower(s.name) CONTAINS toLower(x)) THEN 10 
                ELSE 0 
             END AS scripture_score,
             CASE 
                WHEN size($authors) > 0 AND ANY(x IN $authors WHERE toLower(auth.name) CONTAINS toLower(x) OR toLower(v.author) CONTAINS toLower(x)) THEN 10 
                ELSE 0 
             END AS author_score

        // Filter: If the user provided specific entities, ONLY show results that match at least one
        WHERE (size($scriptures) = 0 OR scripture_score > 0)
          AND (size($authors) = 0 OR author_score > 0)
          AND (size($topics) = 0 OR EXISTS {
                MATCH (v)-[:DISCUSSES]->(t:Topic) WHERE ANY(x IN $topics WHERE toLower(t.name) CONTAINS toLower(x))
          })

        RETURN 
            s.title AS scripture, 
            v.relative_path AS verse_title, 
            v.text as verse_text,
            v.translation AS meaning,
            COALESCE(auth.name, v.author) AS author
        
        ORDER BY (scripture_score + author_score) DESC
        LIMIT 20
    """
    
    context_data = []
    with driver.session() as session:
        result = session.run(cypher_query, **params)
        for record in result:
            context_data.append({
                "scripture": record["scripture"],
                "verse": record["verse_title"],
                "verse_text": record["verse_text"],
                "meaning": record["meaning"],
                "topic": record["author"] if record["author"] else "Search Result"
            })
    
    return context_data, params

# --- 3. The Gradio Interface Function ---

def bhashyam_chat(message, history):
    try:
        # 1. Fetch the Graph context
        context, identified_topics = get_perspectives_from_graph(message)
        print("identified_topics", identified_topics)        
        if not context:
            # Clean up the display for the user
            search_terms = []
            for key, values in identified_topics.items():
                search_terms.extend(values)
            
            yield f"🔍 **Search Query:** {', '.join(search_terms)}\n\n" \
                  f"I couldn't find any specific verses in my graph matching these parameters. " \
                  f"Please try a broader term or check if that scripture is currently migrated."
            return

        # 2. Synthesize the Answer
        # We format the context so the LLM knows which scripture said what.
        formatted_context = ""
        for c in context:
            formatted_context += f"\nFROM [{c['scripture']}] ({c['verse']}): {c['verse_text']} : {c['meaning']} : {c['topic']}\n"

        print("formatted_context", formatted_context)

        system_prompt = f"""
            You are the Bhashyam AI Research Assistant, an expert in Sanatana Dharma.
            
            ### OBJECTIVE:
            Analyze the provided 'CONTEXT FROM GRAPH' to answer the user's question. 
            Note: The context may use Sanskrit terms or traditional concepts that represent modern ideas (e.g., 'Leadership' relates to 'Śreṣṭha', 'Dharma', or 'Loka-saṅgraham').

            ### STRICT RULES:
            1. ONLY use the provided 'CONTEXT FROM GRAPH'. 
            2. INTERPRET the context: If a verse describes qualities of a great person or a king's duty, use that to answer questions about 'Leadership' or 'Governance'.
            3. If the context is TRULY irrelevant (e.g., user asks about "Quantum Physics" and context is about "Conch shells"), state: "My knowledge graph does not currently contain specific verses on this topic. Would you like me to search the web or broaden the topic?"
            4. DO NOT invent new verses. 
            5. CITE the [Scripture Name] and [Verse Title] (e.g., Bhagavad Gita 3.21) for every point made.

            ### CONTEXT FROM GRAPH:
            {formatted_context}

            ### USER QUESTION:
            {message}
            """

        stream = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": message}],
            stream=True
        )

        partial_message = ""
        for chunk in stream:
            if chunk.choices[0].delta.content is not None:
                partial_message += chunk.choices[0].delta.content
                yield partial_message
        return
    except StopIteration:
        # Catch and prevent the RuntimeError
        return 
    except Exception as e:
        yield f"An error occurred: {str(e)}"
        return

# --- 4. Launch Gradio ---

demo = gr.ChatInterface(
    fn=bhashyam_chat,
    title="🕉️ Bhashyam AI - Graph Navigator",
    description="Cross-scriptural search powered by your local Neo4j Knowledge Graph. This UI explores thematic links across different literatures.",
    examples=["Who is the supreme being?", "What is the nature of death?"],
)

if __name__ == "__main__":
    demo.queue().launch()