import json
import requests
import time
import sys
import os
from dotenv import load_dotenv

# --- LOAD CONFIGURATION ---
load_dotenv()
AUTH = (os.getenv("ARCADE_USER"), os.getenv("ARCADE_PASSWORD"))
DB_URL = f"http://{os.getenv('ARCADE_HOST')}:{os.getenv('ARCADE_PORT', '2480')}/api/v1/command/{os.getenv('ARCADE_DB')}"
BATCH_SIZE = 500

# Mapping from label to unique key
UNIQUE_KEYS = {
    "Verse": "global_id",
    "Scripture": "name",
    "Character": "name",
    "Topic": "name",
    "Author": "name",
    "Location": "name",
    "Chapter": "name"
}

def run_script(script):
    payload = {"language": "sqlscript", "command": script}
    try:
        response = requests.post(DB_URL, json=payload, auth=AUTH, timeout=120)
        return response.status_code == 200
    except:
        return False

def import_edges_from_map(jsonl_path, rid_map_path):
    with open(rid_map_path, 'r') as f:
        rid_map = json.load(f)
    
    print(f"--- Loading Edges using RID Map ({len(rid_map)} nodes mapped) ---")
    edge_batch = []
    edge_count = 0
    created_edges = set()

    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            if data.get('type') == 'relationship':
                label = data['label']
                if label not in created_edges:
                    requests.post(DB_URL, json={"language": "sql", "command": f"CREATE EDGE TYPE {label} IF NOT EXISTS"}, auth=AUTH)
                    created_edges.add(label)
                
                # Robust data extraction: start/end can be dicts or strings/ints
                s_data = data['start']
                e_data = data['end']
                
                # Extract IDs
                s_id = str(s_data['id'] if isinstance(s_data, dict) else s_data)
                e_id = str(e_data['id'] if isinstance(e_data, dict) else e_data)
                
                # Extract labels (Use the label from the ID object if possible)
                s_label = s_data['labels'][0] if isinstance(s_data, dict) and 'labels' in s_data else None
                e_label = e_data['labels'][0] if isinstance(e_data, dict) and 'labels' in e_data else None

                # Fallback: if label missing, try to infer or skip
                if s_label and e_label:
                    s_rid = rid_map.get(f"{s_label}:{s_id}")
                    e_rid = rid_map.get(f"{e_label}:{e_id}")
                else:
                    # Attempt simple ID lookup if composite key fails
                    s_rid = rid_map.get(s_id)
                    e_rid = rid_map.get(e_id)
                
                if s_rid and e_rid:
                    edge_batch.append(f"CREATE EDGE {label} FROM {s_rid} TO {e_rid}")
                    edge_count += 1
                else:
                    if not s_rid: print(f"  ⚠️ Missing source RID for {s_label or '???'}:{s_id}")
                    if not e_rid: print(f"  ⚠️ Missing target RID for {e_label or '???'}:{e_id}")
                
                if len(edge_batch) >= BATCH_SIZE:
                    if run_script("BEGIN; " + " ; ".join(edge_batch) + "; COMMIT;"):
                        edge_batch = []
                        time.sleep(0.05)
                        sys.stdout.write(f"\rEdges imported: {edge_count}...")
    
    if edge_batch:
        run_script("BEGIN; " + " ; ".join(edge_batch) + "; COMMIT;")
    print(f"\nPhase 3 Complete. {edge_count} edges imported.")

if __name__ == "__main__":
    if not os.path.exists('rid_map.json'):
        print("ERROR: rid_map.json not found. Run migrate_remote_step2.py first.")
    else:
        import_edges_from_map('../bhashyamai_data_editor/data/bhashyam_export.jsonl', 'rid_map.json')
