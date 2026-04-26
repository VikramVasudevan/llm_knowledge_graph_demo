import base64
import os
import tempfile
import requests
from dotenv import load_dotenv
import gradio as gr
from pyvis.network import Network
from flask import Flask, request, jsonify
import threading
from flask_cors import CORS

load_dotenv()

# --- Configuration ---
ARCADE_URL = f"http://{os.getenv('ARCADE_HOST')}:2480/api/v1/command/{os.getenv('ARCADE_DB', 'BhashyamDB')}"
AUTH = (os.getenv("ARCADE_USER", "root"), os.getenv("ARCADE_PASSWORD"))

# --- Backend Logic ---

def run_arcade_cypher(query, params=None):
    payload = {"language": "cypher", "command": query, "params": params or {}}
    try:
        response = requests.post(ARCADE_URL, json=payload, auth=AUTH, timeout=30)
        print(response.text)
        return response.json().get("result", []) if response.status_code == 200 else []
    except Exception as e:
        print(f"run_arcade_cypher error: {e}")
        return []


# Root chapters: book-level (no incoming PART_OF from another Chapter) — used for UI lists.
def fetch_scripture_choices():
    query = """
    MATCH (p:Chapter)
    WHERE NOT (p)<-[:PART_OF]-(:Chapter)
    RETURN DISTINCT p.scripture AS scripture
    """
    rows = run_arcade_cypher(query)
    return sorted(
        s for s in (r.get("scripture") for r in rows)
        if s
    )


def fetch_prabandham_choices(scripture):
    if not scripture:
        return []
    query = """
    MATCH (p:Chapter)
    WHERE p.scripture = $scripture AND NOT (p)<-[:PART_OF]-(:Chapter)
    RETURN DISTINCT p.name AS name
    """
    rows = run_arcade_cypher(query, {"scripture": scripture})
    return sorted(
        n for n in (r.get("name") for r in rows)
        if n
    )

# --- Gradio + API "The Bridge" ---
# --- 1. THE INITIAL UI FETCH (Load Root + Decades) ---
def get_initial_graph(scripture, prabandham):
    net = Network(height="700px", width="100%", bgcolor="#1a1a1a", font_color="white", directed=True)
    net.barnes_hut(gravity=-3000, central_gravity=0.3, spring_length=150)
    
    # Updated Query: Fetches Root, Author, and the First Level of Chapters (Decades)
    query = f"""
    MATCH (p:Chapter {{name: "{prabandham}", scripture: "{scripture}"}})
    OPTIONAL MATCH (p)<-[r:PART_OF]-(d:Chapter)
    OPTIONAL MATCH (a:Author)-[:CONTRIBUTED_TO]->(p)
    RETURN p, a, collect(d) as decades
    """
    results = run_arcade_cypher(query)
    if not results: return None
    
    res = results[0]
    p_node = res['p']
    p_rid = str(p_node['@rid'])
    
    # Add Root
    net.add_node(p_rid, label=f"📜 {p_node['name']}", color="#E74C3C", size=40, shape="box")
    
    # Add Author
    if res.get('a'):
        a_rid = str(res['a']['@rid'])
        net.add_node(a_rid, label=f"✍️ {res['a']['name']}", color="#9B59B6", size=35, shape="diamond")
        net.add_edge(a_rid, p_rid, color="#9B59B6")

    # Add Decades (The First Level)
    for d in res.get('decades', []):
        if d:
            d_rid = str(d['@rid'])
            net.add_node(d_rid, label=d['name'], color="#F1C40F", size=28)
            net.add_edge(d_rid, p_rid, color="#F1C40F", width=2)

    return net

# --- 2. THE EXPAND ENDPOINT (The "Greedy" Fix) ---
def generate_ui(scripture, prabandham):
    net = get_initial_graph(scripture, prabandham)
    if not net: return "<h3>No data found.</h3>"
    
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".html")
    net.save_graph(temp_file.name)
    with open(temp_file.name, "r", encoding="utf-8") as f:
        html = f.read()

    # JAVASCRIPT: Calls OUR local server instead of ArcadeDB
    drill_down_js = """
    <script type="text/javascript">
    network.on("doubleClick", async function (params) {
        if (params.nodes.length > 0) {
            const nodeId = params.nodes[0];
            
            // We MUST use an absolute URL because the iframe is running from a Data URI
            // Default to localhost, but you can change this to your server's IP if remote
            const rootUrl = window.location.protocol === 'https:' ? 'https://' : 'http://';
            const PROXY_URL = "http://127.0.0.1:5001/expand?rid=";
            
            console.log("Fetching from:", PROXY_URL + encodeURIComponent(nodeId));

            try {
                const response = await fetch(PROXY_URL + encodeURIComponent(nodeId));
                
                if (!response.ok) {
                    throw new Error("HTTP error! status: " + response.status);
                }

                const data = await response.json();
                
                if (!data || data.length === 0) {
                    console.log("No further children or metadata found.");
                    return;
                }

                data.forEach(item => {
                    // 1. Add node if it doesn't exist
                    if (!nodes.get(item.id)) {
                        nodes.add({
                            id: item.id,
                            label: item.label,
                            color: item.color,
                            size: item.size,
                            shape: item.shape || 'dot',
                            font: { color: 'white' }
                        });
                    }
                    // 2. Add edge
                    edges.add({ 
                        from: item.from, 
                        to: item.to, 
                        color: item.color, 
                        width: 1,
                        dashes: item.dashes || false 
                    });
                });
            } catch (err) {
                console.error("Bhashyam Drilldown Error:", err);
            }
        }
    });
    </script>
    """
    
    html = html.replace("</body>", f"{drill_down_js}</body>")
    encoded = base64.b64encode(html.encode()).decode()
    return f'<iframe src="data:text/html;base64,{encoded}" style="width: 100%; height: 750px; border: none;"></iframe>'

# --- The Proxy Server (Flask) ---
# We run a tiny Flask server in a thread to handle the JS requests

app = Flask(__name__)
CORS(app)

@app.route('/expand')
def expand():
    rid_raw = request.args.get('rid')
    print(f"--- Drilling into RID: {rid_raw} ---")

    # In ArcadeDB Cypher, @rid is used directly. 
    # Wrapping it in quotes is usually required for the string-to-RID conversion.
    query = f"""
    MATCH (n) 
    WHERE n.@rid = '{rid_raw}'
    OPTIONAL MATCH (n)<-[r:PART_OF|IN_CHAPTER]-(child)
    OPTIONAL MATCH (n)-[rm:LOCATED_AT|DISCUSSES|MENTIONS|CHARACTER_OF]-(meta)
    RETURN child, meta, labels(child) as cLabels, labels(meta) as mLabels
    """
    
    results = run_arcade_cypher(query)
    payload = []

    for row in results:
        # Handle Children (Chapters/Verses)
        if row.get('child'):
            c = row['child']
            c_rid = str(c['@rid'])
            labels = row.get('cLabels', [])
            
            payload.append({
                "id": c_rid,
                "label": c.get('name') or f"Verse {c.get('unit_index', '')}",
                "color": "#2ECC71" if "Verse" in labels else "#3498DB",
                "size": 12 if "Verse" in labels else 20,
                "from": c_rid, "to": rid_raw
            })

        # Handle Metadata
        if row.get('meta'):
            m = row['meta']
            m_rid = str(m['@rid'])
            labels = row.get('mLabels', [])
            
            payload.append({
                "id": m_rid,
                "label": m.get('name') or m.get('title'),
                "shape": "triangle" if "Location" in labels else "diamond",
                "color": "#E67E22",
                "from": m_rid, "to": rid_raw, "dashes": True
            })

    return jsonify(payload)


def run_flask():
    app.run(port=5001, debug=False, use_reloader=False)

# --- UI ---
def on_scripture_change(scripture):
    prabs = fetch_prabandham_choices(scripture)
    return gr.update(choices=prabs, value=prabs[0] if prabs else None)


_scripture_choices = fetch_scripture_choices()
_default_scripture = _scripture_choices[0] if _scripture_choices else None
_prabandham_choices = (
    fetch_prabandham_choices(_default_scripture) if _default_scripture else []
)

with gr.Blocks() as demo:
    gr.Markdown("# 🕉️ Bhashyam.AI Secure Graph")
    with gr.Row():
        with gr.Column(scale=1):
            s_drop = gr.Dropdown(
                choices=_scripture_choices,
                label="Scripture",
                value=_default_scripture,
            )
            p_drop = gr.Dropdown(
                choices=_prabandham_choices,
                label="Prabandham",
                value=(_prabandham_choices[0] if _prabandham_choices else None),
            )
            btn = gr.Button("Initialize")
        with gr.Column(scale=4):
            out = gr.HTML()

    s_drop.change(on_scripture_change, inputs=s_drop, outputs=p_drop)
    btn.click(generate_ui, inputs=[s_drop, p_drop], outputs=out)

if __name__ == "__main__":
    # Start the proxy server in the background
    threading.Thread(target=run_flask, daemon=True).start()
    demo.launch()