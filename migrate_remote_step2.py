import json
import requests
import sys
import os
from dotenv import load_dotenv

# --- LOAD CONFIGURATION ---
load_dotenv()

DB_URL = f"http://{os.getenv('ARCADE_HOST')}:{os.getenv('ARCADE_PORT', '2480')}/api/v1/command/{os.getenv('ARCADE_DB')}"
AUTH = (os.getenv("ARCADE_USER"), os.getenv("ARCADE_PASSWORD"))

# List all your vertex types
VERTEX_TYPES = ["Verse", "Scripture", "Character", "Topic", "Author", "Location", "Chapter"]

def build_complete_rid_map():
    print("--- Mapping all nodes by neo4j_id using Paged Fetching ---")
    rid_map = {}
    
    for label in VERTEX_TYPES:
        print(f"  - Mapping {label}...")
        offset = 0
        limit = 20000
        while True:
            # ArcadeDB SQL uses SKIP instead of OFFSET
            query = f"SELECT @rid, neo4j_id FROM {label} WHERE neo4j_id IS NOT NULL LIMIT {limit} SKIP {offset}"
            try:
                res = requests.post(DB_URL, json={"language": "sql", "command": query}, auth=AUTH, timeout=300)
                
                if res.status_code == 200:
                    results = res.json().get('result', [])
                    if not results: break # No more records
                    
                    for r in results:
                        # Ensure we access the field name exactly as returned by ArcadeDB
                        rid = r.get('@rid')
                        nid = r.get('neo4j_id')
                        if rid and nid:
                            rid_map[str(nid)] = rid
                    
                    offset += limit
                    sys.stdout.write(f"\r    Mapped {len(rid_map)} total nodes so far...")
                    sys.stdout.flush()
                else:
                    print(f"\n    Warning: Could not map {label}: {res.text}")
                    break
            except Exception as e:
                print(f"\n    Error mapping {label}: {e}")
                break
        print(f"\n    Finished {label}.")
            
    with open('rid_map.json', 'w') as f:
        json.dump(rid_map, f)
    print(f"✅ Successfully mapped {len(rid_map)} nodes to rid_map.json")

if __name__ == "__main__":
    build_complete_rid_map()
