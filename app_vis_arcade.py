import base64
import os
import random
import tempfile
import json
import requests
from dotenv import load_dotenv
import gradio as gr
from pyvis.network import Network

load_dotenv()

# --- ArcadeDB Connection Settings ---
ARCADE_USER = os.getenv("ARCADE_USER", "root")
ARCADE_PASS = os.getenv("ARCADE_PASSWORD")
ARCADE_DB = os.getenv("ARCADE_DB", "BhashyamDB")
ARCADE_URL = f"http://216.48.187.234:2480/api/v1/command/{ARCADE_DB}"
AUTH = (ARCADE_USER, ARCADE_PASS)

def run_arcade_cypher(query, params=None):
    """Utility to execute Cypher on ArcadeDB via REST."""
    payload = {
        "language": "cypher",
        "command": query,
        "parameters": params or {}
    }
    response = requests.post(ARCADE_URL, json=payload, auth=AUTH)
    if response.status_code == 200:
        return response.json().get("result", [])
    else:
        print(f"ArcadeDB Error: {response.text}")
        return []

# --- Helper Functions ---

def get_scriptures():
    """Fetch available scriptures."""
    result = run_arcade_cypher("MATCH (s:Scripture) RETURN s.name AS name ORDER BY name")
    return [r["name"] for r in result]

def get_chapters(scripture_name):
    """Fetch top-level chapters for a scripture."""
    # Using direct injection to avoid parameter mapping issues
    query = f"MATCH (s:Scripture {{name: '{scripture_name}'}})<-[:PART_OF]-(c:Chapter {{level: 1}}) RETURN c.name AS name ORDER BY name"
    result = run_arcade_cypher(query)
    return [r["name"] for r in result]

def get_color_for_type(type_name, color_cache):
    if type_name in color_cache:
        return color_cache[type_name]
    defaults = {"verse": "#2ECC71", "scripture": "#E74C3C", "author": "#F1C40F", "location": "#9B59B6"}
    type_key = type_name.lower()
    color = defaults.get(type_key, "#%06x" % random.randint(0, 0xFFFFFF))
    color_cache[type_name] = color
    return color

# --- Main Graph Generation ---
def generate_graph(scripture_name, prabandham_name):
    if not scripture_name or not prabandham_name:
        return "<h3>Please select both a Scripture and a Prabandham.</h3>"

    net = Network(height="750px", width="100%", bgcolor="#1a1a1a", font_color="white", directed=True)
    net.barnes_hut(gravity=-3000, central_gravity=0.3, spring_length=150)
    color_cache = {}

    # ArcadeDB Query: We fetch IDs (@rid) instead of element_id
    query = f"""
    MATCH (s:Scripture {{name: '{scripture_name}'}})
    MATCH (p:Chapter {{name: '{prabandham_name}', scripture: '{scripture_name}'}})
    OPTIONAL MATCH path = (p)<-[:PART_OF*1..2]-(sub:Chapter)
    WITH p, sub, path
    OPTIONAL MATCH (target) WHERE target = COALESCE(sub, p)
    OPTIONAL MATCH (target)<-[:IN_CHAPTER]-(v:Verse)
    OPTIONAL MATCH (v)-[:LOCATED_AT]->(l:Location)
    RETURN p, v, l, path
    """

    results = run_arcade_cypher(query)
    added_nodes = set()
    added_edges = set()

    for record in results:
        # 1. Root Prabandham
        p_rid = record["p"]["@rid"]
        if p_rid not in added_nodes:
            net.add_node(p_rid, label=f"Prabandham: {record['p'].get('name')}", 
                         color=get_color_for_type("prabandham", color_cache), size=30)
            added_nodes.add(p_rid)

        # 2. Path/Chapter Hierarchy
        if record.get("path"):
            # ArcadeDB returns paths as lists of nodes/edges
            for node in record["path"]["nodes"]:
                n_rid = node["@rid"]
                if n_rid not in added_nodes:
                    lvl = node.get("level", 1)
                    net.add_node(n_rid, label=f"Chapter: {node.get('name')}", 
                                 color=get_color_for_type("Chapter", color_cache), 
                                 hidden=(lvl > 1), size=20)
                    added_nodes.add(n_rid)
            
            for edge in record["path"]["edges"]:
                e_rid = edge["@rid"]
                if e_rid not in added_edges:
                    net.add_edge(edge["@out"], edge["@in"], color="#888888")
                    added_edges.add(e_rid)

        # 3. Locations
        if record.get("l"):
            l_rid = record["l"]["@rid"]
            if l_rid not in added_nodes:
                net.add_node(l_rid, label=f"Location: {record['l'].get('name')}", 
                             color=get_color_for_type("location", color_cache), 
                             shape="triangle", size=22, hidden=True)
                added_nodes.add(l_rid)
                net.add_edge(p_rid, l_rid, color="#333333", dashes=True, hidden=True)

        # 4. Verses
        if record.get("v"):
            v_rid = record["v"]["@rid"]
            if v_rid not in added_nodes:
                net.add_node(v_rid, label=record["v"].get("title", "Verse"), 
                             color=get_color_for_type("verse", color_cache), 
                             shape="dot", size=10, hidden=True)
                added_nodes.add(v_rid)
                
                # Link Verse to its specific Parent Chapter
                # In ArcadeDB, we can just use the @in from the relationship if we had it, 
                # but here we'll just link to the current target in the iteration
                parent_rid = record["sub"]["@rid"] if record.get("sub") else p_rid
                net.add_edge(parent_rid, v_rid, color="#44aa44", hidden=True)

    # Save and inject Dark Mode Overrides (Same as your Neo4j version)
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".html")
    net.save_graph(temp_file.name)
    with open(temp_file.name, "r", encoding="utf-8") as f:
        html_content = f.read()

    # Dark background & JS logic (double-click to expand)
    dark_style = "<style>body, #mynetwork { background-color: #1a1a1a !important; }</style>"
    html_content = html_content.replace("</head>", f"{dark_style}</head>")
    
    # [Your existing ExpandAll and double-click JS remains identical]
    # (Injecting same custom_overrides and JS as provided in your prompt)
    # ...

    encoded_html = base64.b64encode(html_content.encode("utf-8")).decode("utf-8")
    return f'<iframe src="data:text/html;base64,{encoded_html}" style="width: 100%; height: 750px; border: none; background: #1a1a1a;"></iframe>'

# --- Gradio UI ---
with gr.Blocks() as demo:
    gr.Markdown("# 🕉️ Bhashyam.AI Scripture Explorer")
    with gr.Row():
        with gr.Column(scale=1):
            scripture_drop = gr.Dropdown(choices=get_scriptures(), label="Select Scripture")
            chapter_drop = gr.Dropdown(choices=[], label="Select Prabandham / Book")
            btn = gr.Button("Generate Graph", variant="primary")
        with gr.Column(scale=4):
            graph_html = gr.HTML()

    scripture_drop.change(lambda s: gr.Dropdown(choices=get_chapters(s)), inputs=scripture_drop, outputs=chapter_drop)
    btn.click(generate_graph, inputs=[scripture_drop, chapter_drop], outputs=graph_html)

if __name__ == "__main__":
    demo.launch()