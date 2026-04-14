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

def run_arcade_cypher(query):
    """Utility to execute Cypher on ArcadeDB via REST."""
    payload = {"language": "cypher", "command": query, "parameters": {}}
    try:
        response = requests.post(ARCADE_URL, json=payload, auth=AUTH, timeout=30)
        if response.status_code == 200:
            return response.json().get("result", [])
        print(f"ArcadeDB Error: {response.text}")
        return []
    except Exception as e:
        print(f"Request Error: {e}")
        return []

# --- Helper Functions ---

def get_scriptures():
    """Fetch all available scriptures."""
    result = run_arcade_cypher("MATCH (s:Scripture) RETURN s.name AS name ORDER BY name")
    return [r["name"] for r in result]

def get_chapters(scripture_name):
    """Fetch top-level Chapters for the selected scripture."""
    query = f"MATCH (s:Scripture {{name: '{scripture_name}'}})<-[:PART_OF]-(c:Chapter) WHERE c.level = 1 OR c.level IS NULL RETURN c.name AS name ORDER BY name"
    result = run_arcade_cypher(query)
    return [r["name"] for r in result]

def get_color(type_name):
    """Consistent color coding."""
    colors = {
        "prabandham": "#E74C3C", # Red
        "pathu": "#F1C40F",      # Yellow
        "song": "#3498DB",       # Blue
        "author": "#9B59B6",     # Purple
        "location": "#2ECC71"    # Green
    }
    return colors.get(type_name.lower(), "#95A5A6")

# --- Main Graph Generation ---

def generate_graph(scripture_name, prabandham_name):
    s_name = scripture_name.strip()
    p_name = prabandham_name.strip()

    if not s_name or not p_name:
        return "<h3>Please select both a Scripture and a Prabandham.</h3>"

    # Initialize Pyvis
    net = Network(height="750px", width="100%", bgcolor="#1a1a1a", font_color="white", directed=True)
    net.barnes_hut(gravity=-3500, central_gravity=0.3, spring_length=150)

    # Master Query: Returns root, immediate children (c1), and their children (c2)
    query = f"""
    MATCH (p:Chapter) 
    WHERE toLower(p.name) = toLower("{p_name}") 
    AND toLower(p.scripture) = toLower("{s_name}")
    
    OPTIONAL MATCH (c1:Chapter)-[:PART_OF]-(p)
    WHERE c1 <> p
    
    OPTIONAL MATCH (c2:Chapter)-[:PART_OF]-(c1)
    WHERE c2 <> c1 AND c2 <> p
    
    OPTIONAL MATCH (a:Author)-[:CONTRIBUTED_TO]-(p)
    OPTIONAL MATCH (p)-[:LOCATED_AT]-(l:Location)
    
    RETURN p as root, c1, c2, a, l
    """

    results = run_arcade_cypher(query)
    if not results:
        return f"<h3>No data found for '{p_name}'.</h3>"

    added_nodes = set()
    
    for r in results:
        root = r.get("root")
        if not root: continue
        
        # 1. Add Root Prabandham
        root_rid = str(root["@rid"])
        if root_rid not in added_nodes:
            net.add_node(root_rid, label=f"Prabandham: {root['name']}", 
                         color=get_color("prabandham"), size=35, group="root")
            added_nodes.add(root_rid)

        # 2. Add Level 1 (Pathus)
        c1 = r.get("c1")
        if c1:
            c1_rid = str(c1["@rid"])
            if c1_rid not in added_nodes:
                net.add_node(c1_rid, label=f"Pathu: {c1['name']}", 
                             color=get_color("pathu"), size=25, group="pathu")
                net.add_edge(root_rid, c1_rid, color="#888888")
                added_nodes.add(c1_rid)

            # 3. Add Level 2 (Songs) - HIDDEN initially
            c2 = r.get("c2")
            if c2:
                c2_rid = str(c2["@rid"])
                if c2_rid not in added_nodes:
                    net.add_node(c2_rid, label=f"Padigam: {c2['name']}", 
                                 color=get_color("song"), size=15, hidden=True, group="song")
                    net.add_edge(c1_rid, c2_rid, color="#444444", hidden=True)
                    added_nodes.add(c2_rid)

        # 4. Add Authors
        auth = r.get("a")
        if auth:
            a_rid = str(auth["@rid"])
            if a_rid not in added_nodes:
                net.add_node(a_rid, label=f"Author: {auth['name']}", 
                             color=get_color("author"), shape="diamond", group="metadata")
                net.add_edge(a_rid, root_rid, color="#9B59B6")
                added_nodes.add(a_rid)

        # 5. Add Locations
        loc = r.get("l")
        if loc:
            l_rid = str(loc["@rid"])
            if l_rid not in added_nodes:
                net.add_node(l_rid, label=f"Location: {loc['name']}", 
                             color=get_color("location"), shape="triangle", group="metadata")
                net.add_edge(root_rid, l_rid, color="#2ECC71", dashes=True)
                added_nodes.add(l_rid)

    return render_pyvis_html(net)

def render_pyvis_html(net):
    """Converts Pyvis network to HTML with enhanced JS for drill-down."""
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".html")
    net.save_graph(temp_file.name)
    with open(temp_file.name, "r", encoding="utf-8") as f:
        html_content = f.read()
    
    # ENHANCED JS: 
    # 1. Targets neighbors by group ('song') to prevent vanishing parents.
    # 2. Specifically toggles edges between Pathu and Song.
    custom_js = """
    <script type="text/javascript">
    network.on("doubleClick", function (params) {
        if (params.nodes.length > 0) {
            var clickedNodeId = params.nodes[0];
            var neighbors = network.getConnectedNodes(clickedNodeId);
            var updates = [];
            
            neighbors.forEach(function(neighborId) {
                var node = nodes.get(neighborId);
                // ONLY toggle if the neighbor is a "song" (size 15)
                // This prevents Pathus from hiding the Prabandham or Authors.
                if (node && node.size === 15) {
                    var newHiddenState = !node.hidden;
                    updates.push({id: neighborId, hidden: newHiddenState});
                    
                    // Update only the edges connected to this specific song
                    var connectedEdges = network.getConnectedEdges(neighborId);
                    connectedEdges.forEach(function(edgeId) {
                        edges.update({id: edgeId, hidden: newHiddenState});
                    });
                }
            });
            nodes.update(updates);
        }
    });
    </script>
    """
    
    dark_style = """
    <style>
        body, #mynetwork { background-color: #1a1a1a !important; }
        .vis-label { color: white !important; font-family: 'Arial'; }
    </style>
    """
    
    html_content = html_content.replace("</head>", f"{dark_style}</head>")
    html_content = html_content.replace("</body>", f"{custom_js}</body>")
    
    encoded_html = base64.b64encode(html_content.encode("utf-8")).decode("utf-8")
    return f'<iframe src="data:text/html;base64,{encoded_html}" style="width: 100%; height: 750px; border: none;"></iframe>'

# --- UI Layout ---
with gr.Blocks() as demo:
    gr.Markdown("# 🕉️ Bhashyam.AI Graph Explorer")
    with gr.Row():
        with gr.Column(scale=1):
            s_drop = gr.Dropdown(choices=get_scriptures(), label="1. Scripture")
            p_drop = gr.Dropdown(choices=[], label="2. Prabandham")
            btn = gr.Button("Generate Web", variant="primary")
        with gr.Column(scale=4):
            out_html = gr.HTML()

    s_drop.change(lambda s: gr.Dropdown(choices=get_chapters(s)), inputs=s_drop, outputs=p_drop)
    btn.click(generate_graph, inputs=[s_drop, p_drop], outputs=out_html)

if __name__ == "__main__":
    demo.launch()