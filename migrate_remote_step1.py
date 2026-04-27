import json
import requests
import sys
import os
import signal
from dotenv import load_dotenv

# --- LOAD CONFIGURATION ---
load_dotenv()
DB_URL = f"http://{os.getenv('ARCADE_HOST')}:{os.getenv('ARCADE_PORT', '2480')}/api/v1/command/{os.getenv('ARCADE_DB')}"
AUTH = (os.getenv("ARCADE_USER"), os.getenv("ARCADE_PASSWORD"))

# Global flag for graceful exit
INTERRUPTED = False

def signal_handler(sig, frame):
    global INTERRUPTED
    print("\n\n🛑 Interrupt received! Stopping patch...")
    INTERRUPTED = True

signal.signal(signal.SIGINT, signal_handler)

UNIQUE_KEYS = {
    "Verse": "global_id",
    "Scripture": "name",
    "Character": "name",
    "Topic": "name",
    "Author": "name",
    "Location": "name",
    "Chapter": "name"
}

def patch_ids(jsonl_path):
    print("--- Phase: Patching missing neo4j_ids (Killable) ---")
    count = 0
    batch = []
    
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            if INTERRUPTED: break
            try:
                data = json.loads(line)
                if data.get('type') == 'node':
                    label = data['labels'][0]
                    key = UNIQUE_KEYS.get(label, 'name')
                    
                    val = data['properties'].get(key)
                    nid = str(data['id'])

                    if val:
                        safe_val = str(val).replace("'", "''")
                        batch.append(f"UPDATE `{label}` SET neo4j_id = '{nid}' WHERE `{key}` = '{safe_val}'")
                        count += 1
                        
                        if len(batch) >= 100: # Smaller batch for stability
                            script = "BEGIN; " + " ; ".join(batch) + "; COMMIT;"
                            requests.post(DB_URL, json={"language": "sqlscript", "command": script}, auth=AUTH)
                            batch = []
                            sys.stdout.write(f"\rPatched {count} vertices...")
                            sys.stdout.flush()
            except Exception:
                continue
                
    if batch and not INTERRUPTED:
        script = "BEGIN; " + " ; ".join(batch) + "; COMMIT;"
        requests.post(DB_URL, json={"language": "sqlscript", "command": script}, auth=AUTH)
    print(f"\nPatching complete. Total: {count} vertices updated.")

if __name__ == "__main__":
    patch_ids('../bhashyamai_data_editor/data/bhashyam_export.jsonl')
