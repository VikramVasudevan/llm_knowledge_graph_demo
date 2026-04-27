import time
from arcadedb_utils import run_arcade_cypher

def get_progress():
    print(f"{'Scripture':<20} | {'Total':<10} | {'Processed':<10} | {'Progress'}")
    print("-" * 60)
    
    try:
        # Optimized query for ArcadeDB
        query = "SELECT s.name as s_name, v.processed_characters as proc FROM (SELECT FROM Scripture) AS s LEFT JOIN (SELECT FROM Verse) AS v ON s.name = v.scripture_name"
        # Note: Adjusting join condition if needed based on your actual relationship property
        
        # Simpler approach: Fetch data once, calculate locally
        query = "MATCH (s:Scripture)<-[:PART_OF]-(v:Verse) RETURN s.name as s_name, v.processed_characters as proc"
        results = run_arcade_cypher(query)
        
        stats = {}
        for r in results:
            s_name = r.get("s_name", "Unknown")
            is_proc = r.get("proc")
            if s_name not in stats:
                stats[s_name] = {"total": 0, "processed": 0}
            stats[s_name]["total"] += 1
            if is_proc in [True, "true", "True"]:
                stats[s_name]["processed"] += 1
        
        for s_name, data in sorted(stats.items(), key=lambda x: x[1]["total"], reverse=True):
            pct = (data["processed"] / data["total"]) * 100
            print(f"{s_name:<20} | {data['total']:<10} | {data['processed']:<10} | {pct:.2f}%")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    while True:
        get_progress()
        print("\nRefreshing in 10 seconds... (Ctrl+C to stop)")
        time.sleep(10)
