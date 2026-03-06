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

    net = Network(
        height="750px", width="100%", bgcolor=None, font_color="white", directed=True
    )
    net.barnes_hut(gravity=-3000, central_gravity=0.3, spring_length=150)

    color_cache = {}

    query = """
    MATCH (s:Scripture {name: $s_name})
    MATCH (p:Chapter {name: $p_name, scripture: $s_name})
    
    // 1. Hierarchy: Use OPTIONAL so the Prabandham (p) shows up even if it has no children
    OPTIONAL MATCH path = (p)<-[:PART_OF*]-(sub:Chapter)
    
    // 2. Determine which node to look for verses on (either a sub-chapter or the Prabandham itself)
    WITH s, p, path, COALESCE(sub, p) as target_node
    
    // 3. Authors, Verses, and Locations
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
            # 1. Always add the Prabandham node (p) first
            p_node = record["p"]
            if p_node.element_id not in added_nodes:
                net.add_node(
                    p_node.element_id,
                    label=f"Prabandham: {p_node.get('name')}",
                    color=get_color_for_type("prabandham", color_cache),
                    size=30,
                    title="Root Level",
                )
                added_nodes.add(p_node.element_id)

            # 2. Process Hierarchy Path (if it exists)
            path = record["path"]
            if path:
                for node in path.nodes:
                    if node.element_id not in added_nodes:
                        node_type = node.get("type", "Chapter")
                        net.add_node(
                            node.element_id,
                            label=f"{node_type}: {node.get('name')}",
                            color=get_color_for_type(node_type, color_cache),
                            size=25,
                            title=node_type,
                        )
                        added_nodes.add(node.element_id)
                for rel in path.relationships:
                    if rel.element_id not in added_edges:
                        net.add_edge(
                            rel.start_node.element_id,
                            rel.end_node.element_id,
                            color="#888888",
                        )
                        added_edges.add(rel.element_id)

            # ... (Rest of Author, Verse, and Location logic remains the same)

            # 2. Authors (Yellow Stars)
            if (a := record["a"]) and a.element_id not in added_nodes:
                net.add_node(
                    a.element_id,
                    label=f"Author: {a.get('name')}",
                    color=get_color_for_type("author", color_cache),
                    shape="star",
                    size=35,
                )
                added_nodes.add(a.element_id)
                
                # Link Author to Prabandham using the p_node directly
                # This avoids the NoneType error on 'path'
                net.add_edge(
                    a.element_id,
                    p_node.element_id,
                    label="WROTE",
                    color="#F1C40F",
                )
            # 3. Verses (Green Dots - Hidden by Default)
            if (v := record["v"]) and v.element_id not in added_nodes:
                net.add_node(
                    v.element_id,
                    label=v.get("title", "Verse"),
                    color=get_color_for_type("verse", color_cache),
                    shape="dot",
                    size=10,
                    hidden=True,
                )
                added_nodes.add(v.element_id)

                # Link Verse to its Chapter
                edge_res = session.run(
                    """
                    MATCH (v:Verse)-[r:IN_CHAPTER]->(c:Chapter) 
                    WHERE elementId(v) = $vid RETURN elementId(c) as cid, elementId(r) as rid
                """,
                    vid=v.element_id,
                ).single()
                if edge_res and edge_res["rid"] not in added_edges:
                    net.add_edge(
                        v.element_id, edge_res["cid"], color="#44aa44", hidden=True
                    )
                    added_edges.add(edge_res["rid"])

            # 4. Locations (Purple Triangles)
            if (l := record["l"]) and l.element_id not in added_nodes:
                net.add_node(
                    l.element_id,
                    label=f"Location: {l.get('name')}",
                    color=get_color_for_type("location", color_cache),
                    shape="triangle",
                    size=20,
                )
                added_nodes.add(l.element_id)
                net.add_edge(
                    v.element_id, l.element_id, label="MENTIONS", color="#9B59B6"
                )

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
            var connectedNodes = network.getConnectedNodes(nodeId, 'from');
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
