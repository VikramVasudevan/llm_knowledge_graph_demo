import argparse
import sys
import os
import time
from migrate_remote_step1 import patch_ids
from migrate_remote_step2 import build_complete_rid_map
from migrate_remote_step3 import import_edges_from_map

def main():
    parser = argparse.ArgumentParser(description="Orchestrator for ArcadeDB Migration")
    parser.add_argument("--phase", choices=["all", "step1", "step2", "step3"], default="all", help="Phase to execute")
    args = parser.parse_args()

    jsonl_path = '../bhashyamai_data_editor/data/bhashyam_export.jsonl'
    rid_map_path = 'rid_map.json'

    if not os.path.exists(jsonl_path):
        print(f"ERROR: {jsonl_path} not found.")
        sys.exit(1)

    start_time = time.time()

    # Step 1: Patch IDs (only if needed)
    if args.phase in ["all", "step1"]:
        patch_ids(jsonl_path)

    # Step 2: Build RID Map
    if args.phase in ["all", "step2"]:
        build_complete_rid_map()

    # Step 3: Import Edges
    if args.phase in ["all", "step3"]:
        if not os.path.exists(rid_map_path):
            print("ERROR: rid_map.json not found. Run Step 2 first.")
            sys.exit(1)
        import_edges_from_map(jsonl_path, rid_map_path)

    print(f"\nOperation '{args.phase}' complete! Total Time: {int(time.time() - start_time)}s")

if __name__ == "__main__":
    main()
