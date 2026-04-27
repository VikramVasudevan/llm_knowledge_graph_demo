import requests
import os
from dotenv import load_dotenv

load_dotenv()
DB_URL = f"http://{os.getenv('ARCADE_HOST')}:{os.getenv('ARCADE_PORT', '2480')}/api/v1/command/{os.getenv('ARCADE_DB')}"
AUTH = (os.getenv("ARCADE_USER"), os.getenv("ARCADE_PASSWORD"))

def check_counts():
    # Get all vertex types
    payload = {"language": "sql", "command": "SELECT FROM schema:types"}
    res = requests.post(DB_URL, json=payload, auth=AUTH)
    print(res.text)
    types = [t['name'] for t in res.json()['result'] if t['type'] == 'vertex']
    
    print(f"Found {len(types)} vertex types. Checking counts...")
    for t in types:
        count_res = requests.post(DB_URL, json={"language": "sql", "command": f"SELECT count(*) as count FROM {t}"}, auth=AUTH)
        try:
            count = count_res.json()['result'][0]['count']
            print(f"{t}: {count} nodes")
        except:
            print(f"{t}: Could not retrieve count")

check_counts()
