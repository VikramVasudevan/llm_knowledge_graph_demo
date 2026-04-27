import time
import sys
import signal
from arcadedb_utils import (
    run_arcade_cypher,
    run_arcade_sql,
    get_neo4j_counts,
    sync_label,
    sync_relationship,
    ensure_arcade_indexes,
    UNIQUE_KEYS,
    REL_MAPPINGS,
    neo4j_driver
)

# Configuration for large scale move
BATCH_SIZE = 5000  # Increased for throughput

# Global flag for graceful exit
INTERRUPTED = False

def signal_handler(sig, frame):
    global INTERRUPTED
    print("\n\n🛑 Interrupt received! Cleaning up and exiting...")
    INTERRUPTED = True

# Register the signal handler
signal.signal(signal.SIGINT, signal_handler)

def print_progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=50, fill='█', printEnd="\r", start_time=None):
    """
    Custom progress bar with speed and ETA.
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    
    speed_str = ""
    eta_str = ""
    if start_time and iteration > 0:
        elapsed = time.time() - start_time
        speed = iteration / elapsed
        speed_str = f" | {speed:.1f} items/s"
        
        remaining = total - iteration
        eta = remaining / speed
        eta_str = f" | ETA: {time.strftime('%H:%M:%S', time.gmtime(eta))}"

    sys.stdout.write(f'\r{prefix} |{bar}| {percent}% {iteration}/{total}{speed_str}{eta_str} {suffix}')
    sys.stdout.flush()
    if iteration == total:
        print()

def optimized_sync_label(label):
    """Optimized node sync with larger batches and streaming."""
    global INTERRUPTED
    key = UNIQUE_KEYS.get(label, "name")
    
    with neo4j_driver.session() as session:
        result = session.run(f"MATCH (n:{label}) RETURN n")
        
        batch = []
        count = 0
        total = session.run(f"MATCH (n:{label}) RETURN count(n)").single()[0]
        if total == 0: return
        
        start_time = time.time()
        print_progress_bar(0, total, prefix=f"📦 {label}", start_time=start_time)

        for record in result:
            if INTERRUPTED: return False
            
            # Extract dictionary and explicitly filter out any non-serializable objects
            node = record["n"]
            props = dict(node)
            clean_props = {k: v for k, v in props.items() if isinstance(v, (str, int, float, list, dict, bool, type(None)))}
            
            # Perform granular MERGE to avoid parameter parsing complexity
            run_arcade_cypher(f"""
                MERGE (n:{label} {{{key}: $val}})
                SET n += $props
            """, {"val": clean_props.get(key), "props": clean_props})
            
            count += 1
            if count % 100 == 0:
                print_progress_bar(count, total, prefix=f"📦 {label}", start_time=start_time)
        
        print_progress_bar(total, total, prefix=f"📦 {label}", start_time=start_time)
    return True

def optimized_sync_relationship(rel_type):
    """Optimized relationship sync with larger batches and streaming."""
    global INTERRUPTED
    m = REL_MAPPINGS.get(rel_type)
    if not m:
        return True

    with neo4j_driver.session() as session:
        total = session.run(f"MATCH (:{m['src']})-[r:{rel_type}]->(:{m['dst']}) RETURN count(r)").single()[0]
        if total == 0: return True

        result = session.run(f"""
            MATCH (a:{m['src']})-[r:{rel_type}]->(b:{m['dst']})
            RETURN a.{m['src_key']} as src_val, b.{m['dst_key']} as dst_val
        """)
        
        batch = []
        count = 0
        start_time = time.time()
        print_progress_bar(0, total, prefix=f"🔗 {rel_type}", start_time=start_time)

        for record in result:
            if INTERRUPTED: return False
            
            batch.append({"src_val": record["src_val"], "dst_val": record["dst_val"]})
            count += 1
            
            if len(batch) >= BATCH_SIZE:
                run_arcade_cypher(f"""
                    UNWIND $batch AS item
                    MATCH (a:{m['src']} {{{m['src_key']}: item.src_val}}), (b:{m['dst']} {{{m['dst_key']}: item.dst_val}})
                    MERGE (a)-[:{rel_type}]->(b)
                """, {"batch": batch})
                batch = []
                print_progress_bar(count, total, prefix=f"🔗 {rel_type}", start_time=start_time)

        if batch and not INTERRUPTED:
            run_arcade_cypher(f"""
                UNWIND $batch AS item
                MATCH (a:{m['src']} {{{m['src_key']}: item.src_val}}), (b:{m['dst']} {{{m['dst_key']}: item.dst_val}})
                MERGE (a)-[:{rel_type}]->(b)
            """, {"batch": batch})
            print_progress_bar(total, total, prefix=f"🔗 {rel_type}", start_time=start_time)
    return True

def fast_cleanup():
    """Performs a multi-stage fast cleanup using SQL TRUNCATE."""
    print("🧹 Starting Fast Cleanup...")
    
    # 1. Get all types from ArcadeDB
    try:
        # SELECT FROM schema:types is the standard ArcadeDB way to list types
        types_res = run_arcade_sql("SELECT name, type FROM schema:types")
        # Extract type names for Vertex and Edge types
        types_to_truncate = [r["name"] for r in types_res if r.get("type") in ["vertex", "edge"]]
        
        if not types_to_truncate:
            # Fallback: Try known types if schema query returned nothing
            print("  - No types found via schema query, trying known labels...")
            known_labels = list(UNIQUE_KEYS.keys())
            known_rels = list(REL_MAPPINGS.keys())
            types_to_truncate = list(set(known_labels + known_rels))

        # 2. Truncate each type
        for t_name in types_to_truncate:
            if INTERRUPTED: return False
            print(f"  - Truncating type: {t_name}...", end="", flush=True)
            try:
                # TRUNCATE is much faster than DELETE as it bypasses the transaction log for data
                run_arcade_sql(f"TRUNCATE TYPE `{t_name}`")
                print(" Done.")
            except Exception as e:
                # If TRUNCATE fails (e.g. type doesn't exist yet), try a simple DELETE
                try:
                    run_arcade_sql(f"DELETE FROM `{t_name}`")
                    print(" Done (via DELETE).")
                except:
                    print(f" Skipped.")
                
        # 3. Final Cypher cleanup for anything that might have been missed
        if not INTERRUPTED:
            print("  - Performing final Cypher check (quick for empty DB)...", end="", flush=True)
            run_arcade_cypher("MATCH (n) DETACH DELETE n")
            print(" Done.")
            return True
        
    except Exception as e:
        print(f"\n❌ Error during fast cleanup: {e}")
        if not INTERRUPTED:
            print("💡 Falling back to slow cleanup (DETACH DELETE)...")
            run_arcade_cypher("MATCH (n) DETACH DELETE n")
            return True
    return False

def reload_arcade():
    overall_start = time.time()
    print("\n" + "="*60)
    print("🚀 HIGH-PERFORMANCE ARCADEDB RELOAD ENGINE")
    print("   (Press Ctrl+C to safely stop at any time)")
    print("="*60)
    
    try:
        # 1. Fast Cleanup
        print("\n🚀 Step 1: Rapid Cleanup")
        if not fast_cleanup(): return

        # 2. Ensure Indexes AFTER loading
        # Types must exist for indexing, so we do this after data is present.
        print("\n⚡ Step 2: Index Preparation")

        # 3. Fetch Neo4j Counts
        if INTERRUPTED: return
        neo_counts = get_neo4j_counts()

        # 4. Sync Nodes
        print("\n📦 Step 3: Moving Nodes")
        for label in neo_counts["nodes"]:
            if not optimized_sync_label(label): break

        # 5. Sync Relationships
        if not INTERRUPTED:
            print("\n🔗 Step 4: Connecting Relationships")
            for rel_type in neo_counts["relationships"]:
                if not optimized_sync_relationship(rel_type): break

        # Index creation happens here now
        ensure_arcade_indexes()


        if INTERRUPTED:
            print("\n⚠️ Reload was cancelled by user.")
        else:
            duration = time.time() - overall_start
            print("\n" + "="*60)
            print(f"🎉 RELOAD COMPLETE in {duration:.2f} seconds!")
            print("="*60 + "\n")
            
    except Exception as e:
        print(f"\n💥 CRITICAL ERROR: {e}")

if __name__ == "__main__":
    reload_arcade()
