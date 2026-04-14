import os
import json
from dotenv import load_dotenv
from gradio.components.chatbot import ExampleMessage
from neo4j import GraphDatabase
from openai import OpenAI
import gradio as gr
import re
import sqlite3

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


def get_all_characters_table(search_query=""):
    """Fetches characters and their mention counts, excluding specific metadata scriptures."""
    excluded_scriptures = ["yt_metadata"]

    query = """
    MATCH (s:Scripture)<-[:PART_OF]-(v:Verse)-[r:MENTIONS]->(c:Character)
    WHERE NOT s.name IN $excluded_list
    RETURN c.name AS name, count(r) AS verse_count
    ORDER BY verse_count DESC, name ASC
    """

    try:
        with driver.session() as session:
            result = session.run(query, excluded_list=excluded_scriptures)
            # Standardizing to Title Case for display
            all_chars = [[r["name"].title(), r["verse_count"]] for r in result]

            if search_query:
                all_chars = [
                    c for c in all_chars if search_query.lower() in c[0].lower()
                ]
            return all_chars
    except Exception as e:
        return [[f"Error: {e}", 0]]


def get_verses_for_character(evt: gr.SelectData):
    """Fetches all verses linked to a selected character."""
    character_name = evt.value if isinstance(evt.value, str) else evt.value[0]

    query = """
    MATCH (c:Character {name: $char_name})<-[:MENTIONS]-(v:Verse)-[:PART_OF]->(s:Scripture)
    RETURN s.title AS scripture, 
           v.relative_path AS verse, 
           v.text AS text, 
           v.translation AS translation,
           v.word_by_word_native AS wbw
    ORDER BY s.title, v.relative_path
    LIMIT 500
    """

    try:
        with driver.session() as session:
            result = session.run(query, char_name=character_name)
            details = []
            for r in result:
                wbw_str = format_wbw(r["wbw"]) or "N/A"
                details.append(
                    [
                        r["scripture"],
                        r["verse"],
                        r["text"],
                        r["translation"] or "No translation available",
                        wbw_str,
                    ]
                )

            if not details:
                return f"### No verses found for: {character_name}", []

            return f"### 🎭 Verses mentioning: {character_name}", details
    except Exception as e:
        return f"⚠️ Error: {str(e)}", []


def get_all_scriptures_table():
    """Fetches scriptures with aggregated enrichment percentages including empty text verses."""
    query = """
    MATCH (s:Scripture)<-[:PART_OF]-(v:Verse)
    WHERE v.text IS NOT NULL AND v.text <> ""
    WITH s, count(v) AS total_verses
    
    OPTIONAL MATCH (s)<-[:PART_OF]-(v_t:Verse) 
    WHERE v_t.text <> "" AND v_t.translation IS NOT NULL AND v_t.translation <> ""
    WITH s, total_verses, count(v_t) AS t_count
    
    OPTIONAL MATCH (s)<-[:PART_OF]-(v_w:Verse) 
    WHERE v_w.text <> "" AND v_w.word_by_word_native IS NOT NULL 
      AND v_w.word_by_word_native <> "" 
      AND v_w.word_by_word_native <> "[]" 
      AND v_w.word_by_word_native <> []
    WITH s, total_verses, t_count, count(v_w) AS w_count
    
    OPTIONAL MATCH (s)<-[:PART_OF]-(v_top:Verse) 
    WHERE v_top.text <> "" AND EXISTS { (v_top)-[:DISCUSSES]->(:Topic) }
    WITH s, total_verses, t_count, w_count, count(DISTINCT v_top) AS top_count

    OPTIONAL MATCH (s)<-[:PART_OF]-(v_c:Verse) 
    WHERE v_c.text <> "" AND EXISTS { (v_c)-[:MENTIONS]->(:Character) }
    WITH s, total_verses, t_count, w_count, top_count, count(DISTINCT v_c) AS char_count
    
    // IMPORTANT: All variables must be carried into this WITH to be used in the RETURN
    WITH s, total_verses, t_count, w_count, top_count, char_count,
         (t_count * 1.0 / total_verses) * 100 AS p_trans,
         (w_count * 1.0 / total_verses) * 100 AS p_wbw,
         (top_count * 1.0 / total_verses) * 100 AS p_topics,
         (char_count * 1.0 / total_verses) * 100 AS p_chars
         
    RETURN s.title AS title, s.name AS internal_name, total_verses, 
           round((p_trans + p_wbw + p_topics + p_chars) / 4.0, 4) AS overall_enrichment
    ORDER BY overall_enrichment DESC, title ASC
    """
    try:
        with driver.session() as session:
            result = session.run(query)
            return [
                [
                    r["title"],
                    r["total_verses"],
                    f"{r['overall_enrichment']}%",
                    r["internal_name"],
                ]
                for r in result
            ]
    except Exception as e:
        print(f"Neo4j Error: {e}")
        return [[f"Error: {e}", 0, "0%", ""]]


def get_verses_by_scripture(evt: gr.SelectData, scripture_data, filter_mode):
    selected_row = scripture_data.iloc[evt.index[0]]
    scripture_title = selected_row["Scripture Title"]
    internal_name = selected_row["internal_id"]

    query = """
    MATCH (s:Scripture {name: $internal_name})
    
    // 1. Calculate stats excluding empty text
    CALL (s) {
        MATCH (s)<-[:PART_OF]-(v:Verse)
        WHERE v.text IS NOT NULL AND v.text <> ""
        RETURN count(v) AS total
    }
    CALL (s) {
        MATCH (s)<-[:PART_OF]-(v_t:Verse) 
        WHERE v_t.text <> "" AND v_t.translation IS NOT NULL AND v_t.translation <> ""
        RETURN count(v_t) AS t_count
    }
    CALL (s) {
        MATCH (s)<-[:PART_OF]-(v_w:Verse) 
        WHERE v_w.text <> "" AND v_w.word_by_word_native IS NOT NULL 
          AND v_w.word_by_word_native <> "" 
          AND v_w.word_by_word_native <> "[]" 
          AND v_w.word_by_word_native <> []
        RETURN count(v_w) AS w_count
    }
    CALL (s) {
        MATCH (s)<-[:PART_OF]-(v_top:Verse) 
        WHERE v_top.text <> "" AND EXISTS { (v_top)-[:DISCUSSES]->(:Topic) }
        RETURN count(DISTINCT v_top) AS top_count
    }
    // ADDED: Character count call
    CALL (s) {
        MATCH (s)<-[:PART_OF]-(v_char:Verse) 
        WHERE v_char.text <> "" AND EXISTS { (v_char)-[:MENTIONS]->(:Character) }
        RETURN count(DISTINCT v_char) AS char_count
    }

    // 2. Fetch verses with text filter + UI toggle filter
    MATCH (s)<-[:PART_OF]-(v:Verse)
    WHERE v.text IS NOT NULL AND v.text <> ""
      AND ($filter_mode = "Show All" 
           OR (
               v.translation IS NULL 
               OR v.translation = ""
               OR v.word_by_word_native IS NULL 
               OR v.word_by_word_native IN ["", "[]", []]
               OR NOT EXISTS { (v)-[:DISCUSSES]->(:Topic) }
               OR NOT EXISTS { (v)-[:MENTIONS]->(:Character) } // Also filter by missing characters
            ))
    
    WITH s, total, t_count, w_count, top_count, char_count, v
    ORDER BY v.unit_index ASC
    LIMIT 500
    
    
    RETURN 
        total, t_count, w_count, top_count, char_count,
        collect({
            relative_path: v.relative_path,
            text: v.text,
            translation: v.translation,
            wbw: v.word_by_word_native,
            topics: [(v)-[:DISCUSSES]->(t:Topic) | t.name],
            characters: [(v)-[:MENTIONS]->(c:Character) | c.name], // Crucial for the list
            global_id: v.global_id
        }) AS limited_verses
    """
    try:
        with driver.session() as session:
            record = session.run(
                query, internal_name=internal_name, filter_mode=filter_mode
            ).single()
            if not record:
                return "### No data", "", []

            total = record["total"] or 1
            verses_data = record["limited_verses"]

            p_trans = round((record["t_count"] * 1.0 / total) * 100, 4)
            p_wbw = round((record["w_count"] * 1.0 / total) * 100, 4)
            p_topics = round((record["top_count"] * 1.0 / total) * 100, 4)
            p_chars = round((record["char_count"] * 1.0 / total) * 100, 4)  # New Stat

            # Updated Markdown Table with 4 columns
            stats_md = (
                f"| 🎭 Characters | 🏷️ Topics | 🔤 Word-by-Word | 🌐 Translation |\n"
                f"|:---:|:---:|:---:|:---:|\n"
                f"| **{p_chars}%** | **{p_topics}%** | **{p_wbw}%** | **{p_trans}%** |"
            )

            details = []
            for v in verses_data:
                topics_list = ", ".join(v["topics"]) if v["topics"] else "---"
                chars_list = ", ".join(v["characters"]) if v["characters"] else "---"
                wbw_str = format_wbw(v["wbw"])

                # Updated to match 7 columns now
                details.append(
                    [
                        v["relative_path"],
                        v["text"],
                        v["translation"] or "No translation",
                        wbw_str or "N/A",
                        topics_list,  # Topic column
                        chars_list,  # Character column
                        v["global_id"],
                    ]
                )

            mode_label = " (Pending Enrichment)" if filter_mode != "Show All" else ""
            header = f"### 📜 {scripture_title} - {total} Total Verses{mode_label}"
            return header, stats_md, details
    except Exception as e:
        return f"⚠️ Error: {str(e)}", "", []


def update_topic_everywhere(old_name, new_name):
    new_topics_list = [t.strip() for t in new_name.split(",") if t.strip()]
    if not new_topics_list:
        return "⚠️ Error: New name cannot be empty."

    # Use absolute path or ensure this is the SAME db the server uses
    SQLITE_PATH = "../bhashyamai_data_editor/llm_cache.db"
    conn = sqlite3.connect(SQLITE_PATH)
    cursor = conn.cursor()

    try:
        # 1. Update SQLite Cache (Persistent Storage)
        cursor.execute("SELECT hash, topics FROM keywords")
        for h, topics_json in cursor.fetchall():
            topics = json.loads(topics_json)
            if old_name in topics:
                updated = [t for t in topics if t != old_name]
                for nt in new_topics_list:
                    if nt not in updated:
                        updated.append(nt)
                cursor.execute(
                    "UPDATE keywords SET topics = ? WHERE hash = ?",
                    (json.dumps(updated, ensure_ascii=False), h),
                )

        cursor.execute("SELECT hash, data FROM verse_enrichment")
        for h, data_json in cursor.fetchall():
            data = json.loads(data_json)
            if "topics" in data and old_name in data["topics"]:
                existing = data["topics"]
                updated = [t for t in existing if t != old_name]
                for nt in new_topics_list:
                    if nt not in updated:
                        updated.append(nt)
                data["topics"] = updated
                cursor.execute(
                    "UPDATE verse_enrichment SET data = ? WHERE hash = ?",
                    (json.dumps(data, ensure_ascii=False), h),
                )
        conn.commit()

        # 2. Update Neo4j (Graph Storage)
        # We use a DETACH DELETE on the old topic to ensure it's gone
        with driver.session() as session:
            session.run(
                """
                MATCH (oldT:Topic {name: $old_name})
                OPTIONAL MATCH (v:Verse)-[r:DISCUSSES]->(oldT)
                WITH oldT, collect(v) as verses
                
                UNWIND $new_list AS new_t_name
                MERGE (newT:Topic {name: new_t_name})
                
                WITH verses, oldT, newT
                UNWIND verses as v
                MERGE (v)-[:DISCUSSES]->(newT)
                
                WITH oldT
                DETACH DELETE oldT
            """,
                {"old_name": old_name, "new_list": new_topics_list},
            )

        return f"✅ Successfully split '{old_name}' into {new_topics_list}"
    except Exception as e:
        return f"⚠️ Update Error: {str(e)}"
    finally:
        conn.close()


def get_top_10_topics():
    topics = get_all_topics_table()
    # Sort by count descending and take top 10
    top_10 = sorted(topics, key=lambda x: x[1], reverse=True)[:28]

    # Format for gr.Chatbot: List of lists of message dicts
    # Each 'example' is actually a full conversation start
    return [ExampleMessage({"text": t[0]}) for t in top_10]


def get_enrichment_stats():
    query = """
    MATCH (v:Verse) WHERE v.text IS NOT NULL AND v.text <> ""
    WITH count(v) AS total
    CALL () { 
        MATCH (v1:Verse) 
        WHERE v1.text <> "" AND v1.translation IS NOT NULL AND v1.translation <> "" 
        RETURN count(v1) AS with_trans 
    }
    CALL () { 
        MATCH (v2:Verse)  
        WHERE v2.text <> "" 
          AND v2.word_by_word_native IS NOT NULL 
          AND v2.word_by_word_native <> "" 
          AND v2.word_by_word_native <> [] 
          AND v2.word_by_word_native <> "[]"
        RETURN count(v2) AS with_wbw 
    }
    CALL () { 
        MATCH (v3:Verse) 
        WHERE v3.text <> "" AND EXISTS { (v3)-[:DISCUSSES]->(:Topic) }
        RETURN count(DISTINCT v3) AS with_topics 
    }
    CALL () { MATCH (t:Topic) RETURN count(t) AS total_topics }
    CALL () { MATCH (ot:Topic) WHERE NOT (ot)<-[:DISCUSSES]-() RETURN count(ot) AS orphaned_topics }
    RETURN total, with_trans, with_wbw, with_topics, total_topics, orphaned_topics
    """
    try:
        with driver.session() as session:
            record = session.run(query).single()
            if not record:
                return "📊 Database empty."

            total = record["total"] or 1
            p_trans = round((record["with_trans"] / total) * 100, 2)
            p_topics = round((record["with_topics"] / total) * 100, 2)

            return f"""
### 📊 Migration Progress (Valid Verses Only)
- **Total Verses:** {total:,}
- **Enriched:** {p_trans}%
- **Linked Topics:** {p_topics}%

### 🏷️ Topic Stats
- **Total Topics:** {record['total_topics']:,}
- **Orphaned Topics:** {record['orphaned_topics']:,} 
"""
    except Exception as e:
        return f"⚠️ Stats Error: {str(e)}"


def get_verses_for_topic(evt: gr.SelectData):
    global TOPIC_TO_NODES_MAP
    clean_topic_name = evt.value if isinstance(evt.value, str) else evt.value[0]
    raw_names = TOPIC_TO_NODES_MAP.get(clean_topic_name, [])

    if not raw_names:
        return f"### No raw mapping found for: {clean_topic_name}", []

    query = """
    MATCH (t:Topic)<-[:DISCUSSES]-(v:Verse)-[:PART_OF]->(s:Scripture)
    WHERE t.name IN $raw_names
    RETURN s.title AS scripture, 
           v.relative_path AS verse, 
           v.text AS text, 
           v.translation AS translation,
           v.word_by_word_native AS wbw
    LIMIT 5000
    """

    try:
        with driver.session() as session:
            result = session.run(query, raw_names=raw_names)
            details = []
            for r in result:
                # Format Word-by-Word JSON
                wbw_str = format_wbw(r["wbw"]) or "N/A"

                details.append(
                    [
                        r["scripture"],
                        r["verse"],
                        r["text"],
                        r["translation"] or "No translation available",
                        wbw_str,
                    ]
                )

            if not details:
                return f"### No verses found for: {clean_topic_name}", []

            return f"### 📖 Verses discussing: {clean_topic_name}", details
    except Exception as e:
        return f"⚠️ Error: {str(e)}", []


def get_all_topics_table(search_query=""):
    global TOPIC_TO_NODES_MAP
    excluded_scriptures = ["yt_metadata"]

    query = """
    MATCH (s:Scripture)<-[:PART_OF]-(v:Verse)-[r:DISCUSSES]->(t:Topic)
    WHERE NOT s.name IN $excluded_list
    RETURN t.name AS name, count(r) AS verse_count
    """

    numbered_p = re.compile(r"^\d+\.\s*")
    bullet_p = re.compile(r"^[ \t]*[-*:]+[ \t]*")

    try:
        with driver.session() as session:
            result = session.run(query, excluded_list=excluded_scriptures)
            aggregated_topics = {}
            TOPIC_TO_NODES_MAP = {}

            for record in result:
                raw_node_name = record["name"]
                count = record["verse_count"]
                if not raw_node_name:
                    continue

                # 1. Strip brackets/quotes and split by commas/newlines
                clean_name = re.sub(r"[\[\]\"']", "", raw_node_name)
                parts = re.split(r",|\n", clean_name)

                for p in parts:
                    t = p.strip()
                    t = numbered_p.sub("", t)
                    t = bullet_p.sub("", t)
                    display_name = t.strip("*:- ").title()

                    if display_name and len(display_name) > 1:
                        # 2. Aggregate counts
                        aggregated_topics[display_name] = (
                            aggregated_topics.get(display_name, 0) + count
                        )

                        # 3. Map back to Raw Name
                        if display_name not in TOPIC_TO_NODES_MAP:
                            TOPIC_TO_NODES_MAP[display_name] = []
                        if raw_node_name not in TOPIC_TO_NODES_MAP[display_name]:
                            TOPIC_TO_NODES_MAP[display_name].append(raw_node_name)

            # --- THE FIX IS HERE ---
            # aggregated_topics.items() returns (name, count) tuples.
            # We unpack them into a clean list of lists.
            all_topics = [[name, count] for name, count in aggregated_topics.items()]

            if search_query:
                all_topics = [
                    t for t in all_topics if search_query.lower() in t[0].lower()
                ]

            # Sort by Count DESC, then Name ASC
            all_topics.sort(key=lambda x: (-x[1], x[0]))

            return all_topics

    except Exception as e:
        return [[f"Error: {e}", 0]]


def format_wbw(wbw_data):
    """Safely converts WBW data (list or JSON string) into a readable string."""
    if not wbw_data:
        return ""

    # Case 1: Already a list (driver-parsed)
    if isinstance(wbw_data, list):
        items = wbw_data
    # Case 2: It's a string (needs parsing)
    elif isinstance(wbw_data, str):
        if wbw_data.strip().startswith("["):
            try:
                items = json.loads(wbw_data)
            except:
                return wbw_data  # Fallback to raw string
        else:
            return wbw_data  # It's a plain string
    else:
        return str(wbw_data)

    # Format the list items
    try:
        parts = [
            f"{i.get('word', '')}: {i.get('meaning', '')}"
            for i in items
            if isinstance(i, dict)
        ]
        return " | ".join(parts)
    except:
        return str(wbw_data)


# --- 3. Original Perspectives & Chat Logic ---
def get_perspectives_from_graph(user_query, use_fts=True):
    try:
        with driver.session() as session:
            s_result = session.run("MATCH (s:Scripture) RETURN s.name AS name")
            available_scriptures = [record["name"] for record in s_result]
            a_result = session.run("MATCH (a:Author) RETURN a.name AS name LIMIT 100")
            available_authors = [record["name"] for record in a_result]
    except Exception as e:
        print(f"Error fetching metadata: {e}")
        return [], {}

    # 1. Prompt updated to include original language hints
    extraction_prompt = f"""
    Identify entities and search keywords in the user query.
    VALID SCRIPTURES: {available_scriptures}
    VALID AUTHORS: {available_authors}
    
    Question: "{user_query}"
    
    Return JSON: 
    {{
      "scriptures": [], 
      "authors": [],
      "characters": [], 
      "locations": [],
      "topics": [], 
      "search_keywords": ["names", "Sanskrit/Tamil terms", "objects", "concepts"]
    }}
    """

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": extraction_prompt}],
        response_format={"type": "json_object"},
    )
    ents = json.loads(response.choices[0].message.content)

    # 2. Build Search Params
    keywords = ents.get("search_keywords", [])
    search_string = " OR ".join([f'"{k}"~2' for k in keywords if k])

    params = {
        "scriptures": ents.get("scriptures", []),
        "authors": ents.get("authors", []),
        "topics": [t.strip().title() for t in ents.get("topics", [])],
        "characters": [c.strip().title() for c in ents.get("characters", [])],
        "search_string": search_string,
        "use_fts": use_fts,  # Pass this to Cypher
    }

    # 3. Updated Cypher Query with Toggle Support
    cypher_query = """
    // 1. Identify Graph matches
    OPTIONAL MATCH (v_char:Verse)-[:MENTIONS]->(c:Character)
    WHERE toLower(c.name) IN [x IN $characters | toLower(x)]
    WITH collect(DISTINCT v_char) AS char_verses
    
    OPTIONAL MATCH (v_top:Verse)-[:DISCUSSES]->(t:Topic)
    WHERE toLower(t.name) IN [x IN $topics | toLower(x)]
    WITH char_verses, collect(DISTINCT v_top) AS top_verses

    // 2. Fetch FTS nodes (always fetch, we filter the score later)
    OPTIONAL MATCH (v_fts:Verse)
    WHERE $use_fts = true
    CALL {
        WITH v_fts
        CALL db.index.fulltext.queryNodes("verseTextIndex", $search_string) YIELD node, score
        WHERE node = v_fts
        RETURN score as fts_score
    }

    // 3. Combine and Filter
    // We get all verses that are either in our Graph sets OR found via FTS
    MATCH (v:Verse)-[:PART_OF]->(s:Scripture)
    WHERE v IN char_verses 
       OR v IN top_verses 
       OR ($use_fts = true AND EXISTS { 
           CALL db.index.fulltext.queryNodes("verseTextIndex", $search_string) YIELD node 
           WHERE node = v 
           RETURN node 
       })

    // 4. Author Filter
    OPTIONAL MATCH (a:Author)-[:AUTHORED]->(v)
    WHERE (size($scriptures) = 0 OR s.name IN $scriptures)
      AND (size($authors) = 0 OR a.name IN $authors)

    WITH v, s, char_verses, top_verses
    
    // 5. Calculate Final Score
    // Re-calculating FTS score only if enabled
    CALL {
        WITH v
        CALL db.index.fulltext.queryNodes("verseTextIndex", $search_string) YIELD node, score
        WHERE node = v
        RETURN score
        UNION
        RETURN 0.0 AS score
    }
    
    WITH v, s, max(score) as final_fts_score, char_verses, top_verses
    WITH v, s,
         (CASE WHEN $use_fts = true THEN final_fts_score ELSE 0 END) AS base_score,
         (CASE WHEN v IN char_verses THEN 5000 ELSE 0 END) AS char_boost,
         (CASE WHEN v IN top_verses THEN 1000 ELSE 0 END) AS topic_boost
         
    WITH v, s, (base_score + char_boost + topic_boost) AS total_score
    WHERE total_score > 0
    ORDER BY total_score DESC
    
    RETURN s.title AS scripture, v.relative_path AS verse_title, 
           v.text AS verse_text, v.translation AS meaning, 
           v.word_by_word_native AS wbw
    LIMIT 15
    """

    context_data = []
    with driver.session() as session:
        # Fallback logic if search_string is empty
        active_query = (
            cypher_query
            if params["search_string"]
            else """
            MATCH (v:Verse)-[:PART_OF]->(s:Scripture)
            OPTIONAL MATCH (v)-[:DISCUSSES]->(t:Topic)
            WHERE (size($topics) > 0 AND t.name IN $topics)
               OR (size($scriptures) > 0 AND s.name IN $scriptures)
            RETURN s.title AS scripture, v.relative_path AS verse_title, 
                   v.text AS verse_text, v.translation AS meaning, 
                   v.word_by_word_native AS wbw
            ORDER BY size((v)-[:DISCUSSES]->()) DESC
            LIMIT 15
        """
        )

        result = session.run(active_query, **params)
        for record in result:
            raw_meaning = record["meaning"] or ""
            formatted_wbw = format_wbw(record["wbw"])
            if formatted_wbw:
                formatted_wbw = "\nWord-by-Word: " + formatted_wbw

            context_data.append(
                {
                    "scripture": record["scripture"],
                    "verse": record["verse_title"],
                    "verse_text": record["verse_text"],
                    "meaning": f"{raw_meaning}{formatted_wbw}",
                }
            )

    return context_data, params


def bhashyam_chat(message, history, use_fts):
    try:
        context, identified_topics = get_perspectives_from_graph(message, use_fts)
        print("context:\n", context)
        print("identified_topics:\n", identified_topics)
        if not context:
            yield "🔍 No matching verses found."
            return

        formatted_context = ""
        for c in context:
            formatted_context += f"\nFROM [{c['scripture']}] ({c['verse']}): {c['verse_text']} : {c['meaning']}\n"

        system_prompt = f"""
            You are the Bhashyam AI Research Assistant, a scholar of Sanatana Dharma scriptures.
            Use the 'CONTEXT FROM GRAPH' provided below to answer the user's query.

            ### RULES:
            1. COMPARISON: Since you have references from different scriptures, highlight similarities or nuances between them in your summary.
            2. CITATION: Always cite the [Scripture Name] and [Verse Title].
            3. LYRICS: For every verse you reference, you MUST include the original text (Lyrics/Sanskrit/Tamil) followed by its translation.
            4. ATOMICITY: Focus on the specific philosophical topics linked to these verses.

            ### RESPONSE FORMAT:
            - Comparative summary of the concept across the provided scriptures.
            - [Scripture Name] [Verse Title]
            - **Original Text:** [Insert Verse Text here]
            - **Meaning:** [Insert English Translation here]
            
            ### CONTEXT FROM GRAPH:
            {formatted_context}
            """
        stream = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": message},
            ],
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
        # --- Tab 1: Chat ---
        with gr.Tab("💬 Research Chat"):
            with gr.Row():
                with gr.Column(scale=4):
                    # Set type="messages" for compatibility with dict-based history
                    chatbot = gr.Chatbot(
                        height=600,
                        show_label=False,
                        placeholder="### 🔥 Trending Topics",
                        examples=get_top_10_topics(),
                    )
                    with gr.Row():
                        msg = gr.Textbox(
                            placeholder="Ask your query...",
                            label="Ask Bhashyam",
                            scale=9,
                        )
                        submit_btn = gr.Button("Send", variant="primary", scale=1)

                with gr.Column(scale=1, variant="panel"):
                    gr.Markdown("### 🛠️ Search Settings")
                    fts_toggle = gr.Checkbox(
                        label="Enable Full-Text Search",
                        value=True,
                        info="Disable to test pure Graph lookups.",
                    )
                    stats_sidebar = gr.Markdown(get_enrichment_stats())
                    refresh_stats_btn = gr.Button("🔄 Refresh All Stats")

        # --- Tab 2: Topics Index ---
        with gr.Tab("🏷️ Topics Index"):
            with gr.Row():
                with gr.Column(scale=2):
                    gr.Markdown("### 🔍 Filter & Browse")

                    # Added a dedicated Refresh button for the Topic Table
                    refresh_topics_btn = gr.Button("🔄 Refresh Topic List", size="sm")

                    # --- RENAME PANEL ---
                    with gr.Accordion("⚙️ Rename Topic", open=False):
                        old_name_input = gr.Textbox(
                            label="Selected Topic", interactive=False
                        )
                        new_name_input = gr.Textbox(
                            label="New Name", placeholder="Type new name..."
                        )
                        rename_btn = gr.Button(
                            "Apply Rename Everywhere", variant="stop", interactive=False
                        )
                        rename_status = gr.Markdown("")

                    topics_table = gr.Dataframe(
                        headers=["Topic Name", "Verse Count"],
                        datatype=["str", "number"],
                        value=get_all_topics_table(),
                        interactive=False,
                        show_search="search",
                        column_widths=[200, 80],
                    )
                with gr.Column(scale=4):
                    detail_header = gr.Markdown(
                        "### 📖 Topic Details\n*Select a topic on the left to view verses.*"
                    )
                    verse_detail_table = gr.Dataframe(
                        headers=[
                            "Scripture",
                            "Verse ID",
                            "Original Text",
                            "English Translation",
                            "Word-by-Word",
                        ],
                        datatype=["str", "str", "str", "str", "str"],
                        wrap=True,
                        interactive=False,
                        # Redistributed: Metadata (20% total), Content (80% total)
                        column_widths=["10%", "10%", "25%", "25%", "30%"],
                    )

        # --- Tab 2.5: Character Index ---
        with gr.Tab("🎭 Character Index"):
            with gr.Row():
                with gr.Column(scale=2):
                    gr.Markdown("### 🔍 Search Characters")
                    refresh_chars_btn = gr.Button(
                        "🔄 Refresh Character List", size="sm"
                    )

                    chars_table = gr.Dataframe(
                        headers=["Character Name", "Mentions"],
                        datatype=["str", "number"],
                        value=get_all_characters_table(),
                        interactive=False,
                        show_search="search",
                        column_widths=[200, 80],
                    )
                with gr.Column(scale=4):
                    char_detail_header = gr.Markdown(
                        "### 📖 Character Details\n*Select a character on the left to view verses.*"
                    )
                    char_verse_table = gr.Dataframe(
                        headers=[
                            "Scripture",
                            "Verse ID",
                            "Original Text",
                            "English Translation",
                            "Word-by-Word",
                        ],
                        datatype=["str", "str", "str", "str", "str"],
                        wrap=True,
                        interactive=False,
                        column_widths=["10%", "10%", "25%", "25%", "30%"],
                    )

        # --- Tab 3: Scripture Index ---
        with gr.Tab("📜 Scripture Index"):
            with gr.Row():
                with gr.Column(scale=2):
                    gr.Markdown("### 📚 Browse by Scripture")
                    refresh_scripture_btn = gr.Button("🔄 Refresh List", size="sm")

                    scripture_table = gr.Dataframe(
                        headers=[
                            "Scripture Title",
                            "Verses",
                            "Enrichment",
                            "internal_id",
                        ],
                        datatype=["str", "number", "str", "str"],
                        value=get_all_scriptures_table(),
                        interactive=False,
                        show_search="search",
                        column_widths=["55%", "15%", "30%", "0%"],
                    )

                with gr.Column(scale=4):
                    scripture_detail_header = gr.Markdown(
                        "### 📖 Scripture Content\n*Select a scripture on the left.*"
                    )

                    # --- ADDED TOGGLE HERE ---
                    view_mode_toggle = gr.Radio(
                        choices=["Show All", "Pending Enrichment Only"],
                        value="Show All",
                        label="View Mode",
                        info="Filter verses missing Topics, WBW, or Translation",
                    )

                    scripture_enrichment_stats = gr.Markdown(
                        "Select a scripture to see enrichment progress."
                    )
                    scripture_verse_table = gr.Dataframe(
                        headers=[
                            "Verse ID",
                            "Original Text",
                            "English Translation",
                            "Word-by-Word",
                            "Topics",
                            "Characters",  # Added Header
                            "Global Id",
                        ],
                        datatype=[
                            "str",
                            "str",
                            "str",
                            "str",
                            "str",
                            "str",
                            "str",
                        ],  # Added str for Character
                        wrap=True,
                        interactive=False,
                        show_search="search",
                        # Balanced widths: ID(10), Text(15), Trans(20), WBW(20), Topics(15), Chars(15), Global(5)
                        column_widths=["10%", "15%", "20%", "20%", "15%", "15%", "5%"],
                    )
    # --- Event Bindings ---

    # Refresh Statistics Sidebar
    refresh_stats_btn.click(get_enrichment_stats, outputs=stats_sidebar)

    # Refresh Topics Table
    refresh_topics_btn.click(fn=lambda: get_all_topics_table(), outputs=topics_table)

    rename_btn.click(
        fn=update_topic_everywhere,
        inputs=[old_name_input, new_name_input],
        outputs=rename_status,
    ).then(
        fn=lambda: get_all_topics_table(),  # Refresh the table automatically
        outputs=topics_table,
    )

    # 1. Update the helper to return all 4 necessary components
    def select_topic_for_rename(evt: gr.SelectData):
        # Get the clean topic name
        topic_name = evt.value if isinstance(evt.value, str) else evt.value[0]

        # Call the existing verse fetcher logic
        header, details = get_verses_for_topic(evt)

        # Return:
        # 1. Topic name for 'old_name_input'
        # 2. Empty string for 'new_name_input' (to clear previous typing)
        # 3. Header markdown for 'detail_header'
        # 4. Verse list for 'verse_detail_table'
        return topic_name, "", header, details

    # 2. Rebind the select event to the new helper
    # Make sure to remove the old topics_table.select(fn=get_verses_for_topic...)
    # and replace it with this:
    topics_table.select(
        fn=select_topic_for_rename,
        outputs=[old_name_input, new_name_input, detail_header, verse_detail_table],
    )

    # --- Chatbot Event Logic ---
    def user_action(user_message, history):
        # New format: list of dicts
        return "", history + [{"role": "user", "content": user_message}]

    def bot_action(history, use_fts):
        user_message = history[-1]["content"]
        bot_response = bhashyam_chat(user_message, history, use_fts)
        history.append({"role": "assistant", "content": ""})
        for chunk in bot_response:
            history[-1]["content"] = chunk
            yield history

    # NEW: Handler for when a user clicks a Chatbot Example
    def handle_example_click(evt: gr.SelectData, history):
        # evt.value is the dictionary: {"text": "TopicName"}
        topic_name = evt.value["text"]
        user_message = (
            f"What do our sanatana dharma scriptures say on the topic of '{topic_name}'"
        )
        # We reuse the user_action logic to add it to history
        _, updated_history = user_action(user_message, history)
        return updated_history

    # Bind the 'Send' button and 'Enter' key
    submit_btn.click(user_action, [msg, chatbot], [msg, chatbot]).then(
        bot_action, [chatbot, fts_toggle], chatbot
    )
    msg.submit(user_action, [msg, chatbot], [msg, chatbot]).then(
        bot_action, [chatbot, fts_toggle], chatbot
    )
    chatbot.example_select(handle_example_click, chatbot, chatbot).then(
        bot_action, [chatbot, fts_toggle], chatbot
    )
    # Refresh Scripture List
    refresh_scripture_btn.click(fn=get_all_scriptures_table, outputs=scripture_table)

    # Scripture Table Selection Logic
    scripture_table.select(
        fn=get_verses_by_scripture,
        inputs=[scripture_table, view_mode_toggle],  # Added toggle input
        outputs=[
            scripture_detail_header,
            scripture_enrichment_stats,
            scripture_verse_table,
        ],
    )

    # Refresh Character Table
    refresh_chars_btn.click(fn=lambda: get_all_characters_table(), outputs=chars_table)

    # Character Table Selection Logic
    chars_table.select(
        fn=get_verses_for_character,
        outputs=[char_detail_header, char_verse_table],
    )

if __name__ == "__main__":
    demo.queue().launch(
        theme=gr.themes.Default(primary_hue="orange", secondary_hue="gray"), share=False
    )
