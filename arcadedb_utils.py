import os
import json
import requests
from dotenv import load_dotenv
import gradio as gr
import sqlite3
import re

from neo4j import GraphDatabase

# --- 1. Setup & Environment ---
load_dotenv()
# Use your ArcadeDB credentials from your .env
ARCADE_USER = os.getenv("ARCADE_USER", "root")
ARCADE_PASS = os.getenv("ARCADE_PASSWORD")
ARCADE_DB = os.getenv("ARCADE_DB", "BhashyamDB")
ARCADE_HOST = os.getenv("ARCADE_HOST")
ARCADE_URL = f"http://{ARCADE_HOST}:2480/api/v1/command/{ARCADE_DB}"
AUTH = (ARCADE_USER, ARCADE_PASS)

# Persistent session for performance
arcade_session = requests.Session()
arcade_session.auth = AUTH

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def run_arcade_cypher(query, params=None):
    """Refined ArcadeDB Cypher execution with persistent session and timeout."""
    payload = {
        "language": "cypher",
        "command": query,
        "params": params if params else {}
    }
    # 30s timeout to ensure it doesn't hang forever
    response = arcade_session.post(ARCADE_URL, json=payload, timeout=30)
    if response.status_code == 200:
        return response.json().get("result", [])
    else:
        print(f"DEBUG PAYLOAD: {payload}")
        raise Exception(f"ArcadeDB Error: {response.text}")

def run_arcade_sql(command, params=None):
    """Executes native ArcadeDB SQL commands with timeout."""
    payload = {
        "language": "sql",
        "command": command,
        "params": params if params else {}
    }
    response = arcade_session.post(ARCADE_URL, json=payload, timeout=30)
    if response.status_code == 200:
        return response.json().get("result", [])
    else:
        raise Exception(f"ArcadeDB SQL Error: {response.text}")

def ensure_arcade_indexes():
    """Ensures indexes exist for keys used in MERGE operations to prevent performance degradation."""
    print("🛠️ Ensuring ArcadeDB indexes...")
    for label, key in UNIQUE_KEYS.items():
        try:
            # Native ArcadeDB SQL is the preferred way for schema operations
            # syntax: CREATE INDEX IF NOT EXISTS ON <Type> (<prop>) UNIQUE
            run_arcade_sql(f"CREATE INDEX IF NOT EXISTS ON `{label}` (`{key}`) UNIQUE")
            print(f"  - Index ensured: {label}({key})")
        except Exception as e:
            print(f"  - ⚠️ Could not create index for {label}: {e}")

def get_neo4j_counts():
    """Dynamically fetches all node and relationship counts from Neo4j."""
    counts = {"nodes": {}, "relationships": {}}
    with neo4j_driver.session() as session:
        # Node counts per label
        node_res = session.run("MATCH (n) UNWIND labels(n) as label RETURN label, count(*) as count")
        for record in node_res:
            counts["nodes"][record["label"]] = record["count"]
        
        # Relationship counts per type
        rel_res = session.run("MATCH ()-[r]->() RETURN type(r) as type, count(*) as count")
        for record in rel_res:
            counts["relationships"][record["type"]] = record["count"]
    return counts

def get_arcade_counts(progress=None):
    """Dynamically fetches all node and relationship counts from ArcadeDB."""
    counts = {"nodes": {}, "relationships": {}}
    
    # 1. Dynamically fetch ALL node labels from ArcadeDB
    try:
        label_res = run_arcade_cypher("MATCH (n) RETURN DISTINCT labels(n) as labels")
        all_labels = set()
        for r in label_res:
            if r.get("labels"):
                all_labels.update(r["labels"])
        
        # Count each label
        for i, label in enumerate(sorted(all_labels)):
            res = run_arcade_cypher(f"MATCH (n:{label}) RETURN count(n) as count")
            counts["nodes"][label] = res[0]["count"] if res else 0
    except Exception as e:
        print(f"[ERROR] Could not fetch ArcadeDB labels: {e}")

    # 2. Dynamically fetch ALL relationship types from ArcadeDB
    try:
        rel_type_res = run_arcade_cypher("MATCH ()-[r]->() RETURN DISTINCT type(r) as type")
        all_rels = [r["type"] for r in rel_type_res if r.get("type")]
        
        for i, rel in enumerate(sorted(all_rels)):
            res = run_arcade_cypher(f"MATCH ()-[r:{rel}]->() RETURN count(r) as count")
            counts["relationships"][rel] = res[0]["count"] if res else 0
    except Exception as e:
        print(f"[ERROR] Could not fetch ArcadeDB relationships: {e}")
        
    return counts

def get_neo4j_counts():
    """Dynamically fetches all node and relationship counts from Neo4j."""
    counts = {"nodes": {}, "relationships": {}}
    with neo4j_driver.session() as session:
        # Node counts per label
        node_res = session.run("MATCH (n) UNWIND labels(n) as label RETURN label, count(*) as count")
        for record in node_res:
            counts["nodes"][record["label"]] = record["count"]
        
        # Relationship counts per type
        rel_res = session.run("MATCH ()-[r]->() RETURN type(r) as type, count(*) as count")
        for record in rel_res:
            counts["relationships"][record["type"]] = record["count"]
    return counts

def get_arcade_counts(progress=None):
    """Dynamically fetches all node and relationship counts from ArcadeDB."""
    counts = {"nodes": {}, "relationships": {}}
    
    # 1. Dynamically fetch ALL node labels from ArcadeDB
    try:
        label_res = run_arcade_cypher("MATCH (n) RETURN DISTINCT labels(n) as labels")
        all_labels = set()
        for r in label_res:
            if r.get("labels"):
                all_labels.update(r["labels"])
        
        # Count each label
        for i, label in enumerate(sorted(all_labels)):
            res = run_arcade_cypher(f"MATCH (n:{label}) RETURN count(n) as count")
            counts["nodes"][label] = res[0]["count"] if res else 0
    except Exception as e:
        print(f"[ERROR] Could not fetch ArcadeDB labels: {e}")

    # 2. Dynamically fetch ALL relationship types from ArcadeDB
    try:
        rel_type_res = run_arcade_cypher("MATCH ()-[r]->() RETURN DISTINCT type(r) as type")
        all_rels = [r["type"] for r in rel_type_res if r.get("type")]
        
        for i, rel in enumerate(sorted(all_rels)):
            res = run_arcade_cypher(f"MATCH ()-[r:{rel}]->() RETURN count(r) as count")
            counts["relationships"][rel] = res[0]["count"] if res else 0
    except Exception as e:
        print(f"[ERROR] Could not fetch ArcadeDB relationships: {e}")
        
    return counts

# --- Reconciliation & Repair Logic ---

UNIQUE_KEYS = {
    "Verse": "global_id",
    "Scripture": "name",
    "Character": "name",
    "Topic": "name",
    "Author": "name",
    "Location": "name"
}

REL_MAPPINGS = {
    "PART_OF": {"src": "Verse", "src_key": "global_id", "dst": "Scripture", "dst_key": "name"},
    "DISCUSSES": {"src": "Verse", "src_key": "global_id", "dst": "Topic", "dst_key": "name"},
    "MENTIONS": {"src": "Verse", "src_key": "global_id", "dst": "Character", "dst_key": "name"}
}

def get_reconciliation_data():
    """Fetches raw count data from both databases."""
    neo = get_neo4j_counts()
    arcade = get_arcade_counts()
    
    # Also fetch detailed property counts
    with neo4j_driver.session() as session:
        n_trans = session.run("MATCH (v:Verse) WHERE v.translation IS NOT NULL AND v.translation <> '' RETURN count(v) as c").single()["c"]
        n_wbw = session.run("MATCH (v:Verse) WHERE v.word_by_word_native IS NOT NULL AND v.word_by_word_native <> '' AND v.word_by_word_native <> '[]' RETURN count(v) as c").single()["c"]
        n_gid = session.run("MATCH (v:Verse) WHERE v.global_id IS NOT NULL RETURN count(v) as c").single()["c"]

    a_trans = run_arcade_cypher("MATCH (v:Verse) WHERE v.translation IS NOT NULL AND v.translation <> '' RETURN count(v) as c")[0]["c"]
    a_wbw = run_arcade_cypher("MATCH (v:Verse) WHERE v.word_by_word_native IS NOT NULL AND v.word_by_word_native <> '' AND v.word_by_word_native <> '[]' RETURN count(v) as c")[0]["c"]
    a_gid = run_arcade_cypher("MATCH (v:Verse) WHERE v.global_id IS NOT NULL RETURN count(v) as c")[0]["c"]

    detailed = [
        {"metric": "Prop: Verse Translation", "neo": n_trans, "arcade": a_trans, "type": "property", "target": "Verse"},
        {"metric": "Prop: Verse WBW", "neo": n_wbw, "arcade": a_wbw, "type": "property", "target": "Verse"},
        {"metric": "Prop: Verse Global ID", "neo": n_gid, "arcade": a_gid, "type": "property", "target": "Verse"}
    ]
    
    return {"neo": neo, "arcade": arcade, "detailed": detailed}

def generate_recon_markdown(data, active_metric=None, active_progress=0):
    """Generates a markdown table, highlighting the active repair row with a progress bar."""
    report = ["### 🔍 Reconciliation Dashboard\n"]
    report.append("| Metric | Neo4j | ArcadeDB | Status |")
    report.append("| :--- | :---: | :---: | :---: |")

    neo = data["neo"]
    arcade = data["arcade"]
    
    all_labels = sorted(set(neo["nodes"].keys()) | set(arcade["nodes"].keys()))
    all_rels = sorted(set(neo["relationships"].keys()) | set(arcade["relationships"].keys()))
    
    # Rows for Labels
    for label in all_labels:
        n = neo["nodes"].get(label, 0)
        a = arcade["nodes"].get(label, 0)
        metric_name = f"Node: {label}"
        status = get_row_status(metric_name, n, a, active_metric, active_progress)
        report.append(f"| {metric_name} | {n:,} | {a:,} | {status} |")

    # Rows for Relationships
    for rel in all_rels:
        n = neo["relationships"].get(rel, 0)
        a = arcade["relationships"].get(rel, 0)
        metric_name = f"Rel: {rel}"
        status = get_row_status(metric_name, n, a, active_metric, active_progress)
        report.append(f"| {metric_name} | {n:,} | {a:,} | {status} |")

    # Rows for Properties
    for d in data["detailed"]:
        metric_name = d["metric"]
        status = get_row_status(metric_name, d["neo"], d["arcade"], active_metric, active_progress)
        report.append(f"| {metric_name} | {d['neo']:,} | {d['arcade']:,} | {status} |")

    return "\n".join(report)

def get_row_status(name, n, a, active_name, progress):
    if name == active_name:
        # Visual progress bar in markdown
        filled = int(progress * 10)
        bar = "█" * filled + "░" * (10 - filled)
        return f"🛠️ [{bar}] {int(progress*100)}%"
    if n == a:
        return "✅ Match"
    if active_name is not None:
        return "⏳ Waiting..."
    return f"❌ Mismatch ({a-n if a>n else n-a})"

def fix_mismatches_sequentially(progress=gr.Progress()):
    """Iteratively repairs mismatches starting with the lowest counts."""
    print("\n[REPAIR] Starting sequential repair process...")
    data = get_reconciliation_data()
    
    # 1. Identify mismatches and sort by absolute difference
    mismatches = []
    
    # Labels
    for label, n in data["neo"]["nodes"].items():
        a = data["arcade"]["nodes"].get(label, 0)
        if n != a: mismatches.append({"type": "node", "target": label, "count": abs(n-a), "name": f"Node: {label}"})
    
    # Relationships
    for rel, n in data["neo"]["relationships"].items():
        a = data["arcade"]["relationships"].get(rel, 0)
        if n != a: mismatches.append({"type": "rel", "target": rel, "count": abs(n-a), "name": f"Rel: {rel}"})
    
    # Properties (Detailed)
    for d in data["detailed"]:
        if d["neo"] != d["arcade"]:
            mismatches.append({"type": "property", "target": d["target"], "count": abs(d["neo"] - d["arcade"]), "name": d["metric"]})

    # Sort: Lowest counts first (lowest hanging fruit)
    mismatches.sort(key=lambda x: x["count"])
    
    if not mismatches:
        return generate_recon_markdown(data) + "\n\n### 🎉 Everything is already in sync!"

    # 2. Iterate and Fix
    for m in mismatches:
        print(f"[REPAIR] Addressing {m['name']} ({m['count']} items)...")
        
        # Update UI to show we are starting this row
        yield generate_recon_markdown(data, active_metric=m["name"], active_progress=0)
        
        # Perform repair
        if m["type"] == "node":
            for p in sync_label(m["target"]):
                yield generate_recon_markdown(data, active_metric=m["name"], active_progress=p)
        elif m["type"] == "rel":
            for p in sync_relationship(m["target"]):
                yield generate_recon_markdown(data, active_metric=m["name"], active_progress=p)
        elif m["type"] == "property":
            # Property repairs usually covered by node sync, but we can do a targeted one
            for p in sync_label(m["target"]):
                yield generate_recon_markdown(data, active_metric=m["name"], active_progress=p)

        # Refresh data for this specific metric after fix
        data = get_reconciliation_data()
        yield generate_recon_markdown(data)

    print("[REPAIR] Sequential repair complete.")
    return generate_recon_markdown(data) + "\n\n### ✅ Sequential repair complete!"

def sync_label(label):
    """Generic node sync for a label."""
    key = UNIQUE_KEYS.get(label, "name")
    with neo4j_driver.session() as session:
        # Fetch all from Neo4j
        res = session.run(f"MATCH (n:{label}) RETURN n").data()
    
    total = len(res)
    if total == 0: return
    
    BATCH_SIZE = 500
    for i in range(0, total, BATCH_SIZE):
        batch = [r["n"] for r in res[i : i + BATCH_SIZE]]
        # Upsert
        run_arcade_cypher(f"""
            UNWIND $batch AS props
            MERGE (n:{label} {{{key}: props.{key}}})
            SET n += props
        """, {"batch": batch})
        yield (i + len(batch)) / total

def sync_relationship(rel_type):
    """Generic relationship sync for a type."""
    m = REL_MAPPINGS.get(rel_type)
    if not m:
        print(f"[WARN] No mapping for relationship {rel_type}. Skipping.")
        return

    with neo4j_driver.session() as session:
        res = session.run(f"""
            MATCH (a:{m['src']})-[r:{rel_type}]->(b:{m['dst']})
            RETURN a.{m['src_key']} as src_val, b.{m['dst_key']} as dst_val
        """).data()
    
    total = len(res)
    if total == 0: return
    
    BATCH_SIZE = 500
    for i in range(0, total, BATCH_SIZE):
        batch = res[i : i + BATCH_SIZE]
        run_arcade_cypher(f"""
            UNWIND $batch AS item
            MATCH (a:{m['src']} {{{m['src_key']}: item.src_val}}), (b:{m['dst']} {{{m['dst_key']}: item.dst_val}})
            MERGE (a)-[:{rel_type}]->(b)
        """, {"batch": batch})
        yield (i + len(batch)) / total

def reconcile_neo4j_with_arcade(progress=gr.Progress()):
    """
    Compatibility wrapper for the Run Reconciliation button.
    Returns (Markdown Report, gr.update for Fix Button)
    """
    print("[SYSTEM] Running auto-reconciliation...")
    data = get_reconciliation_data()
    md = generate_recon_markdown(data)
    
    # Determine if there are any mismatches
    has_mismatches = False
    
    # 1. Check Nodes
    for label, n in data["neo"]["nodes"].items():
        if n != data["arcade"]["nodes"].get(label, 0):
            has_mismatches = True
            break
            
    # 2. Check Relationships (if no node mismatch)
    if not has_mismatches:
        for rel, n in data["neo"]["relationships"].items():
            if n != data["arcade"]["relationships"].get(rel, 0):
                has_mismatches = True
                break
                
    # 3. Check Detailed Properties (if no other mismatch)
    if not has_mismatches:
        for d in data["detailed"]:
            if d["neo"] != d["arcade"]:
                has_mismatches = True
                break
                
    return md, gr.update(interactive=has_mismatches)

# --- 2. Refactored Functions ---

def get_all_characters_table_from_arcade(search_query=""):
    # ArcadeDB Cypher is very similar
    query = """
    MATCH (s:Scripture)<-[:PART_OF]-(v:Verse)-[r:MENTIONS]->(c:Character)
    RETURN c.name AS name, count(r) AS verse_count
    ORDER BY verse_count DESC
    """
    try:
        result = run_arcade_cypher(query)
        # ArcadeDB result is a list of dicts
        all_chars = [[r["name"].title(), r["verse_count"]] for r in result]
        
        if search_query:
            all_chars = [c for c in all_chars if search_query.lower() in c[0].lower()]
        return all_chars
    except Exception as e:
        return [[f"Error: {e}", 0]]

def get_verses_for_character_from_arcade(evt: gr.SelectData):
    character_name = evt.value if isinstance(evt.value, str) else evt.value[0]

    # Note: We use the same Cypher as Neo4j!
    query = """
    MATCH (c:Character {name: $char_name})<-[:MENTIONS]-(v:Verse)-[:PART_OF]->(s:Scripture)
    RETURN s.title AS scripture, 
           v.relative_path AS verse, 
           v.text AS text, 
           v.translation AS translation,
           v.word_by_word_native AS wbw
    ORDER BY s.title, v.relative_path
    LIMIT 100
    """
    try:
        result = run_arcade_cypher(query, {"char_name": character_name})
        details = []
        for r in result:
            wbw_str = format_wbw(r.get("wbw")) or "N/A"
            details.append([
                r["scripture"],
                r["verse"],
                r["text"],
                r.get("translation") or "No translation available",
                wbw_str,
            ])
        return f"### 🎭 Verses mentioning: {character_name}", details
    except Exception as e:
        return f"⚠️ Error: {str(e)}", []

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

def get_all_scriptures_table_arcade():
    """Fetches scriptures with aggregated enrichment percentages for ArcadeDB."""
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
    WITH s, total_verses, t_count, count(v_w) AS w_count
    
    OPTIONAL MATCH (s)<-[:PART_OF]-(v_top:Verse) 
    WHERE v_top.text <> "" AND size((v_top)-[:DISCUSSES]->(:Topic)) > 0
    WITH s, total_verses, t_count, w_count, count(DISTINCT v_top) AS top_count

    OPTIONAL MATCH (s)<-[:PART_OF]-(v_c:Verse) 
    WHERE v_c.text <> "" AND size((v_c)-[:MENTIONS]->(:Character)) > 0
    WITH s, total_verses, t_count, w_count, top_count, count(DISTINCT v_c) AS char_count
    
    WITH s, total_verses, t_count, w_count, top_count, char_count,
         (t_count * 1.0 / total_verses) * 100 AS p_trans,
         (w_count * 1.0 / total_verses) * 100 AS p_wbw,
         (top_count * 1.0 / total_verses) * 100 AS p_topics,
         (char_count * 1.0 / total_verses) * 100 AS p_chars
         
    RETURN s.title AS title, s.name AS internal_name, total_verses, 
           ((p_trans + p_wbw + p_topics + p_chars) / 4.0) AS overall_enrichment
    ORDER BY overall_enrichment DESC, title ASC
    """
    try:
        # Using your new run_arcade_cypher helper
        result = run_arcade_cypher(query)
        
        return [
            [
                r.get("title", "Unknown"),
                r.get("total_verses", 0),
                f"{round(r.get('overall_enrichment', 0), 2)}%",
                r.get("internal_name", "")
            ]
            for r in result
        ]
    except Exception as e:
        print(f"ArcadeDB Query Error: {e}")
        return [[f"Error: {e}", 0, "0%", ""]]        

def get_verses_by_scripture_arcade(evt: gr.SelectData, scripture_data, filter_mode):
    selected_row = scripture_data.iloc[evt.index[0]]
    scripture_title = selected_row["Scripture Title"]
    internal_name = selected_row["internal_id"]

    # 1. Fetch RAW data for stats (No aggregation in Cypher to avoid GENERATED-key error)
    stats_query = f"""
    MATCH (s:Scripture {{name: '{internal_name}'}})<-[:PART_OF]-(v:Verse)
    WHERE v.text IS NOT NULL AND v.text <> ""
    RETURN 
        v.translation as trans, 
        v.word_by_word_native as wbw,
        size((v)-[:DISCUSSES]->()) as topic_count,
        size((v)-[:MENTIONS]->()) as char_count
    """

    # 2. Fetch Verse Content (Decoupled)
    filter_clause = "AND (v.translation IS NULL OR v.translation = '' OR size((v)-[:DISCUSSES]->()) = 0)" if filter_mode != "Show All" else ""
    content_query = f"""
    MATCH (s:Scripture {{name: '{internal_name}'}})<-[:PART_OF]-(v:Verse)
    WHERE v.text IS NOT NULL AND v.text <> "" {filter_clause}
    WITH v ORDER BY v.unit_index ASC LIMIT 500
    RETURN 
        v.relative_path as relative_path, 
        v.text as text, 
        v.translation as translation, 
        v.word_by_word_native as wbw, 
        v.global_id as global_id,
        [(v)-[:DISCUSSES]->(t:Topic) | t.name] as topics,
        [(v)-[:MENTIONS]->(c:Character) | c.name] as characters
    """

    try:
        # --- Process Stats in Python ---
        raw_stats = run_arcade_cypher(stats_query)
        if not raw_stats: return "### No data", "", []

        total = len(raw_stats)
        t_count = len([r for r in raw_stats if r.get("trans")])
        w_count = len([r for r in raw_stats if r.get("wbw") and r["wbw"] != "[]"])
        top_count = len([r for r in raw_stats if r.get("topic_count", 0) > 0])
        char_count = len([r for r in raw_stats if r.get("char_count", 0) > 0])

        p_trans = round((t_count * 100.0 / total), 2)
        p_wbw = round((w_count * 100.0 / total), 2)
        p_topics = round((top_count * 100.0 / total), 2)
        p_chars = round((char_count * 100.0 / total), 2)

        stats_md = (
            f"| 🎭 Characters | 🏷️ Topics | 🔤 Word-by-Word | 🌐 Translation |\n"
            f"|:---:|:---:|:---:|:---:|\n"
            f"| **{p_chars}%** | **{p_topics}%** | **{p_wbw}%** | **{p_trans}%** |"
        )

        # --- Process Content ---
        verses_res = run_arcade_cypher(content_query)
        details = []
        for v in verses_res:
            details.append([
                v["relative_path"],
                v["text"],
                v.get("translation") or "No translation",
                format_wbw(v.get("wbw")) or "N/A",
                ", ".join(v["topics"]) if v["topics"] else "---",
                ", ".join(v["characters"]) if v["characters"] else "---",
                v.get("global_id", ""),
            ])

        mode_label = " (Pending Enrichment)" if filter_mode != "Show All" else ""
        return f"### 📜 {scripture_title} - {total} Total Verses{mode_label}", stats_md, details

    except Exception as e:
        return f"⚠️ Error: {str(e)}", "", []

def get_enrichment_stats_arcade():
    """Global stats - Pure Graph Traversal (No size() calls)"""
    
    # 1. Total and Enriched Count (Stable)
    q1 = """
    MATCH (v:Verse) 
    WHERE v.text IS NOT NULL AND v.text <> ""
    RETURN count(v) as total, 
           count(v.translation) as with_trans
    """

    # 2. Linked Topics Count (Stable)
    q2 = """
    MATCH (v:Verse)-[:DISCUSSES]->(t:Topic)
    RETURN count(DISTINCT v) as with_topics
    """

    # 3. Topic Global Stats - REFACTORED to avoid GENERATED-key error
    q3 = """
    MATCH (t:Topic)
    WITH count(t) as total_topics
    OPTIONAL MATCH (ot:Topic)
    WHERE NOT (ot)<-[:DISCUSSES]-(:Verse)
    RETURN total_topics, count(DISTINCT ot) as orphaned_topics
    """
    
    try:
        res1 = run_arcade_cypher(q1)
        res2 = run_arcade_cypher(q2)
        res3 = run_arcade_cypher(q3)
        
        if not res1 or not res2 or not res3:
            return "### 📊 Database empty."

        r1, r2, r3 = res1[0], res2[0], res3[0]
        
        total = r1["total"] or 1
        p_trans = round((r1["with_trans"] / total) * 100, 2)
        p_topics = round((r2["with_topics"] / total) * 100, 2)

        return f"""
### 📊 Migration Progress
- **Total Verses:** {total:,}
- **Enriched (Translations):** {p_trans}%
- **Linked Topics:** {p_topics}%

### 🏷️ Topic Stats
- **Total Topics:** {r3['total_topics']:,}
- **Orphaned Topics:** {r3['orphaned_topics']:,} 
"""
    except Exception as e:
        print(f"Stats Error: {e}")
        return f"⚠️ Stats Error: {str(e)}"

def get_verses_for_topic_arcade(evt: gr.SelectData):
    """Fetches all verses linked to a selected topic from ArcadeDB."""
    global TOPIC_TO_NODES_MAP
    clean_topic_name = evt.value if isinstance(evt.value, str) else evt.value[0]
    raw_names = TOPIC_TO_NODES_MAP.get(clean_topic_name, [])

    if not raw_names:
        return f"### No raw mapping found for: {clean_topic_name}", []

    # Cypher remains largely the same, but LIMIT is kept safe for 8GB RAM
    query = """
    MATCH (t:Topic)<-[:DISCUSSES]-(v:Verse)-[:PART_OF]->(s:Scripture)
    WHERE t.name IN $raw_names
    RETURN s.title AS scripture, 
           v.relative_path AS verse, 
           v.text AS text, 
           v.translation AS translation,
           v.word_by_word_native AS wbw
    LIMIT 2000
    """

    try:
        # Use our ArcadeDB helper
        result = run_arcade_cypher(query, {"raw_names": raw_names})
        
        details = []
        for r in result:
            # ArcadeDB might return None for missing fields; use .get()
            wbw_str = format_wbw(r.get("wbw")) or "N/A"

            details.append([
                r.get("scripture", "Unknown"),
                r.get("verse", "N/A"),
                r.get("text", ""),
                r.get("translation") or "No translation available",
                wbw_str,
            ])

        if not details:
            return f"### No verses found for: {clean_topic_name}", []

        return f"### 📖 Verses discussing: {clean_topic_name}", details
        
    except Exception as e:
        print(f"Topic Detail Error: {e}")
        return f"⚠️ Error: {str(e)}", []        

def get_all_topics_table_arcade(search_query=""):
    global TOPIC_TO_NODES_MAP
    
    # --- ADD THESE TWO LINES ---
    numbered_p = re.compile(r"^\d+\.\s*")
    bullet_p = re.compile(r"^[ \t]*[-*:]+[ \t]*")
    # ---------------------------

    excluded = ["yt_metadata"]
    excluded_str = json.dumps(excluded)

    query = f"""
    MATCH (s:Scripture)
    WHERE NOT s.name IN {excluded_str}
    MATCH (s)<-[:PART_OF]-(v:Verse)-[r:DISCUSSES]->(t:Topic)
    RETURN t.name AS name, count(r) AS verse_count
    """
    
    try:
        # Call without the second 'params' argument
        result = run_arcade_cypher(query)
        
        aggregated_topics = {}
        TOPIC_TO_NODES_MAP = {}
        
        for record in result:
            # ArcadeDB returns a list of dictionaries
            raw_node_name = record.get("name")
            count = record.get("verse_count", 0)
            
            if not raw_node_name:
                continue

            # 1. Strip brackets/quotes and split (Your existing logic is perfect)
            clean_name = re.sub(r"[\[\]\"']", "", str(raw_node_name))
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

                    # 3. Map back to Raw Name for the Detail view lookup
                    if display_name not in TOPIC_TO_NODES_MAP:
                        TOPIC_TO_NODES_MAP[display_name] = []
                    if raw_node_name not in TOPIC_TO_NODES_MAP[display_name]:
                        TOPIC_TO_NODES_MAP[display_name].append(raw_node_name)

        # Unpack into list of lists for Gradio Dataframe
        all_topics = [[name, count] for name, count in aggregated_topics.items()]

        if search_query:
            all_topics = [
                t for t in all_topics if search_query.lower() in t[0].lower()
            ]

        # Sort by Count DESC, then Name ASC
        all_topics.sort(key=lambda x: (-x[1], x[0]))

        return all_topics

    except Exception as e:
        print(f"Topic Table Error: {e}")
        return [[f"Error: {e}", 0]]        

def get_perspectives_from_graph_arcade(client, user_query, conversation_history=None, use_fts=True):
    try:
        # 1. Fetch metadata using ArcadeDB helper
        s_result = run_arcade_cypher("MATCH (s:Scripture) RETURN s.name AS name")
        available_scriptures = [record["name"] for record in s_result]

        a_result = run_arcade_cypher("MATCH (a:Author) RETURN a.name AS name LIMIT 100")
        available_authors = [record["name"] for record in a_result]
    except Exception as e:
        print(f"Error fetching metadata: {e}")
        return [], {}

    # 2. LLM Extraction with History
    history_str = ""
    if conversation_history:
        history_str = "HISTORY:\n" + "\n".join([f"{h['role'].upper()}: {h['content']}" for h in conversation_history[-3:]])

    extraction_prompt = f"""
    Identify entities and search keywords in the user query.
    VALID SCRIPTURES: {available_scriptures}
    VALID AUTHORS: {available_authors}
    {history_str}
    Question: "{user_query}"
    
    INSTRUCTIONS:
    - Extract Scriptures, Authors, Characters, and Topics.
    - DO NOT include "Topic" or "same topic" as a topic name.
    - If the user refers to "the same topic", look at the HISTORY to find the topic mentioned in previous turns and include that in the 'topics' list.
    
    Return JSON: {{ "scriptures": [], "authors": [], "characters": [], "topics": [], "search_keywords": [] }}
    """
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": extraction_prompt}],
        response_format={"type": "json_object"},
    )
    ents = json.loads(response.choices[0].message.content)

    # 3. Build Search Params
    keywords = [k.strip() for k in ents.get("search_keywords", []) if k.strip()]
    search_string = " OR ".join([f"{k}~2" for k in keywords])

    extracted_topics = [t.strip().title() for t in ents.get("topics", [])]
    
    # Fallback: If no formal topics, look at keywords for likely candidates,
    # excluding those we already identified as scriptures or authors.
    final_topics = extracted_topics
    if not final_topics and keywords:
        candidates = [k for k in keywords if k.title() not in available_scriptures and k.title() not in available_authors]
        if candidates:
            final_topics = [candidates[0].title()]

    params = {
        "scriptures": ents.get("scriptures", []),
        "authors": ents.get("authors", []),
        "topics": final_topics,
        "characters": [c.strip().title() for c in ents.get("characters", [])],
        "search_string": search_string,
        "keywords": keywords
    }

    context_data_map = {} # Using a map to deduplicate results

    def add_to_context(records):
        for record in records:
            # Create a unique key for deduplication
            key = f"{record.get('scripture')}:{record.get('verse_title')}"
            if key not in context_data_map:
                context_data_map[key] = {
                    "scripture": record.get("scripture", "Unknown"),
                    "verse": record.get("verse_title", "N/A"),
                    "verse_text": record.get("verse_text", ""),
                    "meaning": f"{record.get('meaning') or ''}\n{format_wbw(record.get('wbw'))}",
                }

    try:
        # PHASE 1: Graph-based relationship matches (Highly reliable)
        if params["characters"] or params["topics"] or params["scriptures"]:
            print("Querying based on metadata ...")
            
            # If scriptures are provided, enforce them. If not, allow any.
            if params["scriptures"]:
                scripture_filter = "s.name IN $scriptures"
            else:
                scripture_filter = "1=1" # No restriction

            graph_query = f"""
            MATCH (v:Verse)-[:PART_OF]->(s:Scripture)
            WHERE 
                {scripture_filter}
                AND (size($characters) = 0 OR size([(v)-[:MENTIONS]->(c:Character) WHERE toLower(c.name) IN [x IN $characters | toLower(x)] | c]) > 0)
                AND (size($topics) = 0 OR size([(v)-[:DISCUSSES]->(t:Topic) WHERE toLower(t.name) IN [x IN $topics | toLower(x)] | t]) > 0)
            RETURN s.title AS scripture, v.relative_path AS verse_title, 
                   v.text AS verse_text, v.translation AS meaning, 
                   v.word_by_word_native AS wbw
            LIMIT 20
            """
            graph_results = run_arcade_cypher(graph_query, params)
            add_to_context(graph_results)

        # PHASE 2: Full-Text Search (if enabled and we need more results)
        if use_fts and params["search_string"] and len(context_data_map) < 15:
            print("Querying based on FTS ...")
            # We use a very simple query for FTS to avoid parser syntax issues
            fts_query = """
            MATCH (v:Verse)-[:PART_OF]->(s:Scripture)
            WHERE v SEARCH $search_string
            RETURN s.title AS scripture, v.relative_path AS verse_title, 
                   v.text AS verse_text, v.translation AS meaning, 
                   v.word_by_word_native AS wbw
            LIMIT 15
            """
            try:
                fts_results = run_arcade_cypher(fts_query, {"search_string": params["search_string"]})
                add_to_context(fts_results)
            except Exception as fts_err:
                print(f"FTS Search Error (Skipping): {fts_err}")
                # Fallback to simple CONTAINS if FTS fails
                if keywords:
                    fallback_query = """
                    MATCH (v:Verse)-[:PART_OF]->(s:Scripture)
                    WHERE ANY(k IN $keywords WHERE toLower(v.text) CONTAINS toLower(k))
                    RETURN s.title AS scripture, v.relative_path AS verse_title, 
                           v.text AS verse_text, v.translation AS meaning, 
                           v.word_by_word_native AS wbw
                    LIMIT 10
                    """
                    fallback_results = run_arcade_cypher(fallback_query, {"keywords": keywords})
                    add_to_context(fallback_results)

        # PHASE 3: Fallback to general interesting verses if still empty
        if not context_data_map:
            fallback_query = """
            MATCH (v:Verse)-[:PART_OF]->(s:Scripture)
            WHERE size((v)-[:DISCUSSES]->()) > 0
            RETURN s.title AS scripture, v.relative_path AS verse_title, 
                   v.text AS verse_text, v.translation AS meaning, 
                   v.word_by_word_native AS wbw
            ORDER BY size((v)-[:DISCUSSES]->()) DESC
            LIMIT 10
            """
            fallback_results = run_arcade_cypher(fallback_query)
            add_to_context(fallback_results)

    except Exception as e:
        print(f"Chat Search Error: {e}")

    return list(context_data_map.values()), params

def update_topic_everywhere_arcade(old_name, new_name):
    new_topics_list = [t.strip() for t in new_name.split(",") if t.strip()]
    if not new_topics_list:
        return "⚠️ Error: New name cannot be empty."

    # 1. Update SQLite Cache (Stays the same)
    SQLITE_PATH = "../bhashyamai_data_editor/llm_cache.db"
    conn = sqlite3.connect(SQLITE_PATH)
    cursor = conn.cursor()

    try:
        # Updating keywords table
        cursor.execute("SELECT hash, topics FROM keywords")
        for h, topics_json in cursor.fetchall():
            topics = json.loads(topics_json)
            if old_name in topics:
                updated = [t for t in topics if t != old_name]
                for nt in new_topics_list:
                    if nt not in updated: updated.append(nt)
                cursor.execute("UPDATE keywords SET topics = ? WHERE hash = ?", 
                               (json.dumps(updated, ensure_ascii=False), h))

        # Updating verse_enrichment table
        cursor.execute("SELECT hash, data FROM verse_enrichment")
        for h, data_json in cursor.fetchall():
            data = json.loads(data_json)
            if "topics" in data and old_name in data["topics"]:
                updated = [t for t in data["topics"] if t != old_name]
                for nt in new_topics_list:
                    if nt not in updated: updated.append(nt)
                data["topics"] = updated
                cursor.execute("UPDATE verse_enrichment SET data = ? WHERE hash = ?", 
                               (json.dumps(data, ensure_ascii=False), h))
        conn.commit()

        # 2. Update ArcadeDB (Graph Storage)
        # We perform this in a single script for atomicity
        arcade_script = """
        // Find verses linked to the old topic
        MATCH (oldT:Topic {name: $old_name})
        OPTIONAL MATCH (v:Verse)-[r:DISCUSSES]->(oldT)
        WITH oldT, collect(v) as verses
        
        // Create/Merge the new topics
        UNWIND $new_list AS new_t_name
        MERGE (newT:Topic {name: new_t_name})
        
        // Relink verses to new topics
        WITH verses, oldT, newT
        UNWIND verses as v
        MERGE (v)-[:DISCUSSES]->(newT)
        
        // Remove the old topic and its relationships
        WITH DISTINCT oldT
        DETACH DELETE oldT
        """
        
        run_arcade_cypher(arcade_script, {"old_name": old_name, "new_list": new_topics_list})

        return f"✅ Successfully split '{old_name}' into {new_topics_list} in SQLite and ArcadeDB"
        
    except Exception as e:
        print(f"Update Error: {e}")
        return f"⚠️ Update Error: {str(e)}"
    finally:
        conn.close()    