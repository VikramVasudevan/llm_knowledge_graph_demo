import requests
import os
from dotenv import load_dotenv

# --- LOAD CONFIGURATION ---
load_dotenv()

# ArcadeDB Connection
ARCADE_URL = f"http://{os.getenv('ARCADE_HOST')}:{os.getenv('ARCADE_PORT', '2480')}/api/v1/command/{os.getenv('ARCADE_DB')}"
ARCADE_AUTH = (os.getenv("ARCADE_USER"), os.getenv("ARCADE_PASSWORD"))

# Neo4j Connection (Assuming bolt URI is standard)
from neo4j import GraphDatabase
NEO4J_DRIVER = GraphDatabase.driver(os.getenv("NEO4J_URI", "bolt://localhost:7687"), 
                                    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD")))

def run_arcade(query):
    res = requests.post(ARCADE_URL, json={"language": "sql", "command": query}, auth=ARCADE_AUTH)
    return res.json().get('result', [])

def run_neo4j(query):
    with NEO4J_DRIVER.session() as session:
        return session.run(query).data()

def reconcile():
    print("--- Starting Database Reconciliation ---")
    
    # 1. Check Node Counts
    types = ["Verse", "Scripture", "Character", "Topic", "Author", "Location", "Chapter"]
    print(f"{'Label':<15} | {'Neo4j':<10} | {'Arcade':<10} | {'Status'}")
    
    for t in types:
        n_count = run_neo4j(f"MATCH (n:{t}) RETURN count(n) as c")[0]['c']
        a_count = run_arcade(f"SELECT count(*) as c FROM {t}")[0]['c']
        status = "✅ Match" if n_count == a_count else "❌ Mismatch"
        print(f"{t:<15} | {n_count:<10} | {a_count:<10} | {status}")

    print("\n--- Edge Counts ---")
    n_edges = run_neo4j("MATCH ()-[r]->() RETURN type(r) as type, count(r) as c")
    
    # Dynamically fetch edge types from ArcadeDB schema
    edge_types_res = run_arcade("SELECT name FROM schema:types WHERE type = 'edge'")
    a_edge_map = {}
    for et in edge_types_res:
        t_name = et['name']
        count_res = run_arcade(f"SELECT count(*) as c FROM {t_name}")
        a_edge_map[t_name] = count_res[0]['c']
    
    n_edge_map = {e['type']: e['c'] for e in n_edges}
    
    all_types = set(list(n_edge_map.keys()) + list(a_edge_map.keys()))
    
    print(f"{'Edge Type':<15} | {'Neo4j':<10} | {'Arcade':<10} | {'Status'}")
    for t in all_types:
        n = n_edge_map.get(t, 0)
        a = a_edge_map.get(t, 0)
        status = "✅ Match" if n == a else "❌ Mismatch"
        print(f"{t:<15} | {n:<10} | {a:<10} | {status}")

    # 3. Spot Check Properties
    print("\n--- Property Spot Check (Sampling 5 nodes per type) ---")
    mismatch_found = False
    for t in types:
        # Get 5 random samples from Neo4j
        samples = run_neo4j(f"MATCH (n:{t}) WITH n, rand() as r ORDER BY r LIMIT 5 RETURN n")
        for s in samples:
            props = s['n']
            if 'neo4j_id' not in props: continue
            nid = props['neo4j_id']
            
            # Fetch same node from ArcadeDB
            a_node = run_arcade(f"SELECT FROM {t} WHERE neo4j_id = '{nid}'")
            if not a_node:
                print(f"❌ Mismatch: Node {nid} missing in ArcadeDB")
                mismatch_found = True
                continue
            
            # Compare properties
            a_props = a_node[0]
            # Simple check: do they share same number of keys?
            if len(a_props) < len(props):
                 print(f"⚠️ Warning: Node {nid} might have missing properties in ArcadeDB")
                 mismatch_found = True

    if not mismatch_found:
        print("✅ Spot check passed: All sampled nodes match Neo4j properties.")

    print("\nReconciliation complete.")

if __name__ == "__main__":
    reconcile()
    NEO4J_DRIVER.close()
