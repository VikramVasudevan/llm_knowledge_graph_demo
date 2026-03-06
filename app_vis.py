import base64
import os
import random
import tempfile
from dotenv import load_dotenv
import gradio as gr
from neo4j import GraphDatabase
from pyvis.network import Network

load_dotenv()

# --- Neo4j Connection ---
URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASSWORD = os.getenv("NEO4J_PASSWORD")
driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

# --- Helper Functions ---


def get_scriptures():
    """Fetch available scriptures for the sidebar."""
    with driver.session() as session:
        result = session.run("MATCH (s:Scripture) RETURN s.name AS name ORDER BY name")
        return [record["name"] for record in result]


def get_chapters(scripture_name):
    """Fetch top-level chapters (Prabandhams) for a scripture."""
    with driver.session() as session:
        query = """
        MATCH (s:Scripture {name: $name})<-[:PART_OF]-(c:Chapter {level: 1}) 
        RETURN c.name AS name ORDER BY name
        """
        result = session.run(query, name=scripture_name)
        return [record["name"] for record in result]


def get_color_for_type(type_name, color_cache):
    """Generates and caches colors based on node type."""
    if type_name in color_cache:
        return color_cache[type_name]

    defaults = {
        "verse": "#2ECC71",  # Green
        "scripture": "#E74C3C",  # Red
        "author": "#F1C40F",  # Yellow
        "location": "#9B59B6",  # Purple
    }

    type_key = type_name.lower()
    if type_key in defaults:
        color = defaults[type_key]
    else:
        random.seed(type_name)
        color = "#%06x" % random.randint(0, 0xFFFFFF)

    color_cache[type_name] = color
    return color


# --- Main Graph Generation ---
def generate_graph(scripture_name, prabandham_name):
    if not scripture_name or not prabandham_name:
        return "<h3>Please select both a Scripture and a Prabandham.</h3>"

    net = Network(height="750px", width="100%", bgcolor=None, font_color="white", directed=True)
    net.barnes_hut(gravity=-3000, central_gravity=0.3, spring_length=150)

    color_cache = {}

    query = """
    MATCH (s:Scripture {name: $s_name})
    MATCH (p:Chapter {name: $p_name, scripture: $s_name})
    OPTIONAL MATCH path = (p)<-[:PART_OF*]-(sub:Chapter)
    WITH s, p, path, COALESCE(sub, p) as target_node
    OPTIONAL MATCH (target_node)<-[:IN_CHAPTER]-(v:Verse)
    OPTIONAL MATCH (a:Author)-[:CONTRIBUTED_TO]->(p)
    OPTIONAL MATCH (v)-[:LOCATED_AT]->(l:Location)
    RETURN p, path, v, a, l
    """

    with driver.session() as session:
        results = session.run(query, s_name=scripture_name, p_name=prabandham_name)
        added_nodes = set()
        added_edges = set()

        for record in results:
            p_node = record["p"]
            # 1. Root
            if p_node.element_id not in added_nodes:
                net.add_node(p_node.element_id, label=f"Prabandham: {p_node.get('name')}", 
                             color=get_color_for_type("prabandham", color_cache), size=30)
                added_nodes.add(p_node.element_id)

            # 2. Hierarchy Path
            path = record["path"]
            if path:
                for node in path.nodes:
                    if node.element_id not in added_nodes:
                        node_type = node.get("type", "Chapter")
                        node_level = node.get("level", 1)
                        # Hide anything below the first level
                        net.add_node(node.element_id, label=f"{node_type}: {node.get('name')}", 
                                     color=get_color_for_type(node_type, color_cache), 
                                     hidden=(node_level > 1), size=20)
                        added_nodes.add(node.element_id)
                for rel in path.relationships:
                    if rel.element_id not in added_edges:
                        # Ensure edges are hidden if the child is hidden
                        edge_hidden = (rel.start_node.get("level", 2) > 1)
                        net.add_edge(rel.end_node.element_id, rel.start_node.element_id, 
                                     color="#888888", hidden=edge_hidden)
                        added_edges.add(rel.element_id)

            # 3. Locations (Purple Triangles)
            if (l := record["l"]) and l.element_id not in added_nodes:
                net.add_node(l.element_id, label=f"Location: {l.get('name')}", 
                             color=get_color_for_type("location", color_cache), 
                             shape="triangle", size=22, hidden=True)
                added_nodes.add(l.element_id)
                
                # IMPORTANT: Link Location to Root Prabandham so it shows on first drill-down
                net.add_edge(p_node.element_id, l.element_id, color="#333333", dashes=True, hidden=True)

            # 4. Verses (Green Dots)
            if (v := record["v"]) and v.element_id not in added_nodes:
                net.add_node(v.element_id, label=v.get("title", "Verse"), 
                             color=get_color_for_type("verse", color_cache), 
                             shape="dot", size=10, hidden=True)
                added_nodes.add(v.element_id)
                
                # FIND THE PARENT CHAPTER TO LINK DOWNWARD
                # We need to link Chapter -> Verse so double-clicking the chapter reveals the verse
                # Your DB has (v)-[:IN_CHAPTER]->(c), so we reverse the arrow for the UI
                edge_res = session.run("MATCH (v:Verse)-[:IN_CHAPTER]->(c:Chapter) WHERE elementId(v) = $vid RETURN elementId(c) as cid", vid=v.element_id).single()
                if edge_res:
                    net.add_edge(edge_res["cid"], v.element_id, color="#44aa44", hidden=True)

                # Link Verse -> Location
                if l:
                    net.add_edge(v.element_id, l.element_id, label="MENTIONS", color="#9B59B6", hidden=True)

    # Save and inject JS
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".html")
    net.save_graph(temp_file.name)
    with open(temp_file.name, "r", encoding="utf-8") as f:
        html_content = f.read()

    # 3. FORCE DARK BACKGROUND in the HTML content
    # This overrides the browser's default white iframe background
    dark_style = """
    <style>
        body { background-color: #1a1a1a !important; margin: 0; padding: 0; }
        #mynetwork { background-color: #1a1a1a !important; border: none; }
    </style>
    """
    html_content = html_content.replace("</head>", f"{dark_style}</head>")

    # --- GLOBAL CSS & JS OVERRIDE ---
    custom_overrides = """
    <style>
        /* Force Dark Background on everything */
        body, #mynetwork, canvas { 
            background-color: #1a1a1a !important; 
        }
        
        /* Force all text to be readable (Off-White) */
        * {
            color: #e0e0e0 !important;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif !important;
        }

        /* Styling for the Expand All button specifically */
        .vis-button-custom {
            position: fixed; 
            top: 15px; 
            left: 15px; 
            z-index: 1000; 
            padding: 10px 20px; 
            background-color: #2ecc71 !important; 
            color: white !important; 
            border: none; 
            border_radius: 5px; 
            cursor: pointer; 
            font-weight: bold;
            box-shadow: 0 2px 5px rgba(0,0,0,0.5);
        }
        
        /* Fix for tooltips/popups that might still be white-on-white */
        div.vis-tooltip {
            background-color: #333333 !important;
            color: #ffffff !important;
            border: 1px solid #555 !important;
            padding: 8px !important;
            border-radius: 4px !important;
        }
    </style>

    <button class="vis-button-custom" onclick="expandAllNodes()">Expand All</button>

    <script type="text/javascript">
    network.on("doubleClick", function (params) {
        if (params.nodes.length > 0) {
            var nodeId = params.nodes[0];
            // 'to' finds all nodes that the clicked node points AT (its children)
            var connectedNodes = network.getConnectedNodes(nodeId, 'to');
            var updateArray = [];
            
            connectedNodes.forEach(function(childId) {
                var node = nodes.get(childId);
                var isHidden = !node.hidden;
                updateArray.push({id: childId, hidden: isHidden});
                
                var connectedEdges = network.getConnectedEdges(childId);
                connectedEdges.forEach(function(edgeId) {
                    edges.update({id: edgeId, hidden: isHidden});
                });
            });
            nodes.update(updateArray);
        }
    });

    // Filtering logic for Locations
    network.on("selectNode", function (params) {
        var selectedId = params.nodes[0];
        var selectedNode = nodes.get(selectedId);
        
        if (selectedNode.shape === 'triangle') {
            // 'from' finds verses that point TO this location
            var relatedVerseIds = network.getConnectedNodes(selectedId, 'from');
            
            var allNodes = nodes.get().map(node => {
                if (node.id === selectedId || relatedVerseIds.includes(node.id)) {
                    return {id: node.id, opacity: 1, hidden: false};
                } else {
                    return {id: node.id, opacity: 0.1};
                }
            });
            nodes.update(allNodes);
        }
    });

    // Reset Filter on background click
    network.on("deselectNode", function (params) {
        var allNodes = nodes.get().map(node => ({id: node.id, opacity: 1}));
        nodes.update(allNodes);
    });

    function expandAllNodes() {
        var allNodes = nodes.get().map(node => ({id: node.id, hidden: false}));
        var allEdges = edges.get().map(edge => ({id: edge.id, hidden: false}));
        nodes.update(allNodes);
        edges.update(allEdges);
    }
    </script>
    """
    
    # Replace the body tag with our custom overrides
    html_content = html_content.replace("</body>", f"{custom_overrides}</body>")

    encoded_html = base64.b64encode(html_content.encode("utf-8")).decode("utf-8")
    iframe_src = f"data:text/html;base64,{encoded_html}"
    return f"""
    <iframe 
        src="{iframe_src}" 
        style="width: 100%; height: 750px; border: 1px solid #333; border-radius: 8px; background-color: #1a1a1a; box-shadow: 0 4px 6px rgba(0,0,0,0.3);"
    ></iframe>
    """


# --- Gradio UI ---

with gr.Blocks() as demo:
    gr.Markdown("# 🕉️ Bhashyam.AI Scripture Explorer")

    with gr.Row():
        with gr.Column(scale=1):
            scripture_drop = gr.Dropdown(
                choices=get_scriptures(), label="Select Scripture"
            )
            chapter_drop = gr.Dropdown(choices=[], label="Select Prabandham / Book")
            btn = gr.Button("Generate Graph", variant="primary")

        with gr.Column(scale=4):
            graph_html = gr.HTML()

    def update_chapters(scripture):
        return gr.Dropdown(choices=get_chapters(scripture))

    scripture_drop.change(update_chapters, inputs=scripture_drop, outputs=chapter_drop)
    btn.click(generate_graph, inputs=[scripture_drop, chapter_drop], outputs=graph_html)

if __name__ == "__main__":
    demo.launch(theme=gr.themes.Soft())
