import os
import time
import requests
import pandas as pd
import boto3
import json
import re
import io
from botocore.config import Config
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
ELECTION_URL_PREFIX = os.getenv("ELECTION_URL_PREFIX")
if not ELECTION_URL_PREFIX:
    print("ELECTION_URL_PREFIX not provided. Exiting.")
    import sys
    sys.exit(0)

ELECTION_ID = os.getenv("ELECTION_ID", "2026_RESULTS")

R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
R2_ENDPOINT_URL = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com" if R2_ACCOUNT_ID else None

DELAY_BETWEEN_REQUESTS = float(os.getenv("DELAY_BETWEEN_REQUESTS", "1.0"))

STATE_CONFIG = {
    "West Bengal": {"code": "S25", "count": 294, "trend_pages": 30},
    "Tamil Nadu": {"code": "S22", "count": 234, "trend_pages": 24},
    "Assam": {"code": "S03", "count": 126, "trend_pages": 13},
    "Kerala": {"code": "S11", "count": 140, "trend_pages": 14},
    "Puducherry": {"code": "U07", "count": 30, "trend_pages": 3}
}

CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)
TRENDS_CACHE_FILE = CACHE_DIR / "trends_cache.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def get_r2_client():
    if not all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
        return None
    return boto3.client("s3", endpoint_url=R2_ENDPOINT_URL, aws_access_key_id=R2_ACCESS_KEY_ID,
                        aws_secret_access_key=R2_SECRET_ACCESS_KEY, config=Config(signature_version="s3v4"), region_name="auto")

def upload_to_r2(content, key, content_type="text/csv"):
    client = get_r2_client()
    if not client: return False
    try:
        client.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=content, ContentType=content_type)
        print(f"Uploaded to R2: {key}")
        return True
    except Exception as e:
        print(f"Error uploading to R2: {e}"); return False

def get_from_r2(key):
    client = get_r2_client()
    if not client: return None
    try:
        response = client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        return response['Body'].read().decode('utf-8')
    except: return None

def update_consolidated_file(new_df, key):
    """Appends new data to a consolidated history file in R2."""
    existing_content = get_from_r2(key)
    if existing_content:
        existing_df = pd.read_csv(io.StringIO(existing_content))
        # Drop duplicates if scraping logic overlaps, keeping newest
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        # We might want to keep history, so we don't drop duplicates unless they are truly identical
        # but for election trends, timestamp makes them unique.
    else:
        combined_df = new_df
    
    upload_to_r2(combined_df.to_csv(index=False), key)

def fetch_party_wise(state_code: str) -> pd.DataFrame | None:
    """Fetches party-wise results for a state."""
    url = f"https://results.eci.gov.in/{ELECTION_URL_PREFIX}/partywiseresult-{state_code}.htm"
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code != 200: return None
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.select_one('#div1 table')
        if not table: return None
        
        rows = []
        for tr in table.find_all('tr')[1:]: # Skip header
            tds = tr.find_all('td')
            if len(tds) < 4: continue
            rows.append({
                "Party": tds[0].text.strip(),
                "Won": tds[1].text.strip(),
                "Leading": tds[2].text.strip(),
                "Total": tds[3].text.strip(),
                "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        return pd.DataFrame(rows) if rows else None
    except Exception as e:
        print(f"Error party-wise {state_code}: {e}"); return None

def fetch_state_trends(state_name: str, state_code: str, page_no: int) -> list:
    """Fetches trends (summary of multiple ACs). Columns: Const, Leading Cand, Party, Trailing Cand, Party, Margin, Status."""
    url = f"https://results.eci.gov.in/{ELECTION_URL_PREFIX}/statewise{state_code}{page_no}.htm"
    results = []
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code != 200: return []
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", {"id": "ElectionResult"}) or soup.find("table")
        if not table: return []
        for tr in table.find_all('tr'):
            tds = tr.find_all('td', recursive=False)
            if len(tds) < 7: continue
            ac_link = tds[0].find('a')
            ac_no = int(re.search(r'(\d+)\.htm', ac_link['href']).group(1)) if ac_link else None
            results.append({
                "State": state_name, "State_Code": state_code,
                "Constituency_Name": tds[0].text.strip(), "Constituency_No": ac_no,
                "Leading_Candidate": tds[2].text.strip(), "Leading_Party": tds[3].text.strip(),
                "Trailing_Candidate": tds[4].text.strip(), "Trailing_Party": tds[5].text.strip(),
                "Margin": tds[6].text.strip(), "Status": tds[7].text.strip() if len(tds) > 7 else "In Progress",
                "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
    except Exception as e: print(f"Error trends {state_name} P{page_no}: {e}")
    return results

def fetch_constituency_details(state_name: str, state_code: str, const_no: int) -> pd.DataFrame | None:
    """Fetches candidate-wise details. Columns: SN, Candidate, Party, EVM, Postal, Total, %."""
    url = f"https://results.eci.gov.in/{ELECTION_URL_PREFIX}/Constituencywise{state_code}{const_no}.htm"
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code != 200: return None
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.select_one('div.table-responsive > table') or soup.find("table")
        if not table: return None
        rows = []
        tbody = table.find('tbody') or table
        for tr in tbody.find_all('tr'):
            tds = tr.find_all('td', recursive=False)
            if len(tds) < 6: continue
            rows.append({
                "SN": tds[0].text.strip(), "Candidate": tds[1].text.strip(), "Party": tds[2].text.strip(),
                "EVM_Votes": tds[3].text.strip(), "Postal_Votes": tds[4].text.strip(),
                "Total_Votes": tds[5].text.strip(), "Vote_Percentage": tds[6].text.strip() if len(tds) > 6 else "",
                "State": state_name, "Constituency_No": const_no, "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        return pd.DataFrame(rows) if rows else None
    except Exception as e: print(f"Error detail {state_name} AC {const_no}: {e}"); return None

def scrape_all():
    print(f"Cycle starting: {datetime.now()}")
    cache = json.load(open(TRENDS_CACHE_FILE)) if TRENDS_CACHE_FILE.exists() else {}
    all_state_trends = []
    all_party_summaries = []

    for state, config in STATE_CONFIG.items():
        # 1. Party-wise (Latest and Consolidated)
        party_df = fetch_party_wise(config["code"])
        if party_df is not None:
            party_df["State"] = state
            upload_to_r2(party_df.to_csv(index=False), f"results/party_wise_{config['code']}_latest.csv")
            update_consolidated_file(party_df, f"results/party_wise_{config['code']}_history.csv")
            all_party_summaries.append(party_df)

        # 2. State-wise Trends
        state_trends = []
        for p in range(1, config["trend_pages"] + 1):
            page_results = fetch_state_trends(state, config["code"], p)
            if not page_results: break
            state_trends.extend(page_results)
            time.sleep(DELAY_BETWEEN_REQUESTS)
        
        if state_trends:
            trends_df = pd.DataFrame(state_trends)
            upload_to_r2(trends_df.to_csv(index=False), f"results/trends_{config['code']}_latest.csv")
            update_consolidated_file(trends_df, f"results/trends_{config['code']}_history.csv")
            all_state_trends.extend(state_trends)

            # 3. Targeted Constituency-wise Details
            for trend in state_trends:
                ac_key = f"{trend['State_Code']}_{trend['Constituency_No']}"
                old = cache.get(ac_key, {})
                if (trend['Leading_Candidate'] != old.get('Leading_Candidate') or 
                    trend['Margin'] != old.get('Margin') or trend['Status'] != old.get('Status')):
                    
                    print(f"Update: {state} AC {trend['Constituency_No']}")
                    detail_df = fetch_constituency_details(state, trend['State_Code'], trend['Constituency_No'])
                    if detail_df is not None:
                        upload_to_r2(detail_df.to_csv(index=False), f"results/{trend['State_Code']}/{trend['Constituency_No']}/latest.csv")
                        update_consolidated_file(detail_df, f"results/{trend['State_Code']}/{trend['Constituency_No']}/history.csv")
                    cache[ac_key] = trend
                    time.sleep(DELAY_BETWEEN_REQUESTS)

    # Global summaries
    if all_state_trends:
        global_trends_df = pd.DataFrame(all_state_trends)
        upload_to_r2(global_trends_df.to_csv(index=False), "results/constituency_status_latest.csv")
        update_consolidated_file(global_trends_df, "results/constituency_status_history.csv")
    
    if all_party_summaries:
        global_party_df = pd.concat(all_party_summaries, ignore_index=True)
        upload_to_r2(global_party_df.to_csv(index=False), "results/party_summary_latest.csv")
        update_consolidated_file(global_party_df, "results/party_summary_history.csv")

    with open(TRENDS_CACHE_FILE, 'w') as f: json.dump(cache, f, indent=2)
    print(f"Cycle complete: {datetime.now()}")

def main():
    global ELECTION_URL_PREFIX
    while True:
        prefix = os.getenv("ELECTION_URL_PREFIX")
        if not prefix:
            print(f"[{datetime.now()}] ELECTION_URL_PREFIX not provided. Waiting...")
            time.sleep(60)
            continue
        
        ELECTION_URL_PREFIX = prefix
        try:
            scrape_all()
        except Exception as e:
            print(f"Error during scrape: {e}")
            
        print(f"[{datetime.now()}] Cycle complete. Sleeping for 10 minutes...")
        time.sleep(600)

if __name__ == "__main__":
    main()
