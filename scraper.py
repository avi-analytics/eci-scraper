import io
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path

import boto3
import pandas as pd
import requests
from botocore.config import Config
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

SCHEMA_VERSION = "2026-05-03"
DEFAULT_ELECTION_URL_PREFIX = "ResultAcGenMay2026"
BASE_RESULTS_URL = "https://results.eci.gov.in"

# --- CONFIGURATION (Globals updated in main) ---
ELECTION_URL_PREFIX = DEFAULT_ELECTION_URL_PREFIX
ELECTION_ID = "2026_RESULTS"
R2_ACCOUNT_ID = None
R2_ACCESS_KEY_ID = None
R2_SECRET_ACCESS_KEY = None
R2_BUCKET_NAME = None
R2_ENDPOINT_URL = None
HTTP_SESSION = None

STATE_CONFIG = {
    "West Bengal": {"code": "S25", "count": 294, "trend_pages": 30, "dashboard_key": "wb"},
    "Tamil Nadu": {"code": "S22", "count": 234, "trend_pages": 24, "dashboard_key": "tn"},
    "Assam": {"code": "S03", "count": 126, "trend_pages": 13, "dashboard_key": "as"},
    "Kerala": {"code": "S11", "count": 140, "trend_pages": 14, "dashboard_key": "kl"},
    "Puducherry": {"code": "U07", "count": 30, "trend_pages": 3, "dashboard_key": "py"},
}

CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/91.0.4472.124 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
}

DELAY_BETWEEN_REQUESTS = 0.0
POLL_INTERVAL_SECONDS = 30.0


def now_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(message: str):
    print(message, flush=True)


def get_election_base_url() -> str:
    return f"{BASE_RESULTS_URL}/{ELECTION_URL_PREFIX}"


def get_index_url() -> str:
    return f"{get_election_base_url()}/index.htm"


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def build_object_key(*parts: str) -> str:
    clean_parts = []
    for part in parts:
        if part is None:
            continue
        text = str(part).strip("/")
        if text:
            clean_parts.append(text)
    return "/".join(clean_parts)


def get_root_prefix() -> str:
    return build_object_key("elections", ELECTION_ID)


def get_cache_file() -> Path:
    return CACHE_DIR / f"trends_cache_{ELECTION_ID}.json"


def ensure_cache_shape(cache: dict | None) -> dict:
    shaped = cache if isinstance(cache, dict) else {}
    shaped.setdefault("partywise", {})
    shaped.setdefault("statewide", {})
    return shaped


def normalize_constituency_no(value) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    if not digits:
        raise ValueError(f"Unable to normalize constituency number from {value!r}")
    return digits.zfill(3)


def make_constituency_key(state_code: str, constituency_no) -> str:
    return f"{state_code}-{normalize_constituency_no(constituency_no)}"


def get_summary_dataset_keys(dataset_name: str) -> dict:
    prefix = build_object_key(get_root_prefix(), "summary", dataset_name)
    return {
        "current": build_object_key(prefix, "current.csv"),
        "history": build_object_key(prefix, "history.csv"),
    }


def get_state_dataset_keys(state_code: str, dataset_name: str) -> dict:
    prefix = build_object_key(get_root_prefix(), "states", state_code, dataset_name)
    return {
        "current": build_object_key(prefix, "current.csv"),
        "history": build_object_key(prefix, "history.csv"),
    }


def get_constituency_dataset_keys(state_code: str, constituency_no, dataset_name: str = "candidatewise") -> dict:
    constituency_segment = normalize_constituency_no(constituency_no)
    prefix = build_object_key(
        get_root_prefix(),
        "states",
        state_code,
        "constituencies",
        constituency_segment,
        dataset_name,
    )
    return {
        "current": build_object_key(prefix, "current.csv"),
        "history": build_object_key(prefix, "history.csv"),
    }


def get_manifest_key() -> str:
    return build_object_key(get_root_prefix(), "manifest.json")


def build_manifest() -> dict:
    root_prefix = get_root_prefix()
    states = {}
    for state_name, config in STATE_CONFIG.items():
        state_code = config["code"]
        states[state_code] = {
            "state_name": state_name,
            "state_code": state_code,
            "dashboard_key": config["dashboard_key"],
            "seat_count": config["count"],
            "partywise": {
                "current_key": get_state_dataset_keys(state_code, "partywise")["current"],
                "history_key": get_state_dataset_keys(state_code, "partywise")["history"],
            },
            "statewide_trends": {
                "current_key": get_state_dataset_keys(state_code, "statewide-trends")["current"],
                "history_key": get_state_dataset_keys(state_code, "statewide-trends")["history"],
            },
            "constituencies_prefix": build_object_key(root_prefix, "states", state_code, "constituencies"),
        }

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now_timestamp(),
        "election_id": ELECTION_ID,
        "root_prefix": root_prefix,
        "summary": {
            "partywise": {
                "current_key": get_summary_dataset_keys("partywise")["current"],
                "history_key": get_summary_dataset_keys("partywise")["history"],
            },
            "statewide_trends": {
                "current_key": get_summary_dataset_keys("statewide-trends")["current"],
                "history_key": get_summary_dataset_keys("statewide-trends")["history"],
            },
        },
        "states": states,
        "templates": {
            "partywise_current": "elections/{election_id}/states/{state_code}/partywise/current.csv",
            "partywise_history": "elections/{election_id}/states/{state_code}/partywise/history.csv",
            "statewide_trends_current": (
                "elections/{election_id}/states/{state_code}/statewide-trends/current.csv"
            ),
            "statewide_trends_history": (
                "elections/{election_id}/states/{state_code}/statewide-trends/history.csv"
            ),
            "candidatewise_current": (
                "elections/{election_id}/states/{state_code}/constituencies/"
                "{constituency_no_padded}/candidatewise/current.csv"
            ),
            "candidatewise_history": (
                "elections/{election_id}/states/{state_code}/constituencies/"
                "{constituency_no_padded}/candidatewise/history.csv"
            ),
            "summary_partywise_current": "elections/{election_id}/summary/partywise/current.csv",
            "summary_partywise_history": "elections/{election_id}/summary/partywise/history.csv",
            "summary_statewide_trends_current": (
                "elections/{election_id}/summary/statewide-trends/current.csv"
            ),
            "summary_statewide_trends_history": (
                "elections/{election_id}/summary/statewide-trends/history.csv"
            ),
        },
        "constituency_no_format": "3-digit zero-padded string",
    }


def update_config():
    global ELECTION_URL_PREFIX, ELECTION_ID, R2_ACCOUNT_ID, R2_ACCESS_KEY_ID
    global R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME, R2_ENDPOINT_URL
    global DELAY_BETWEEN_REQUESTS, POLL_INTERVAL_SECONDS

    load_dotenv(override=True)
    ELECTION_URL_PREFIX = os.getenv("ELECTION_URL_PREFIX", DEFAULT_ELECTION_URL_PREFIX)
    ELECTION_ID = os.getenv("ELECTION_ID", "2026_RESULTS")
    R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
    R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
    R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
    R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
    R2_ENDPOINT_URL = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com" if R2_ACCOUNT_ID else None
    DELAY_BETWEEN_REQUESTS = float(os.getenv("DELAY_BETWEEN_REQUESTS", "0.0"))
    POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "30"))


def get_r2_client():
    if not all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
        return None
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def get_http_session() -> requests.Session:
    global HTTP_SESSION
    if HTTP_SESSION is None:
        HTTP_SESSION = requests.Session()
        HTTP_SESSION.headers.update(HEADERS)
    return HTTP_SESSION


def bootstrap_http_session() -> bool:
    session = get_http_session()
    try:
        response = session.get(get_index_url(), timeout=15)
        log(
            f"Bootstrapped ECI session with HTTP {response.status_code}; "
            f"cookies={len(session.cookies)}"
        )
        return response.status_code == 200
    except Exception as exc:
        log(f"Error bootstrapping ECI session: {exc}")
        return False


def fetch_url(url: str, referer: str | None = None) -> requests.Response | None:
    session = get_http_session()
    headers = {}
    if referer:
        headers["Referer"] = referer

    try:
        response = session.get(url, headers=headers, timeout=15)
    except Exception as exc:
        log(f"HTTP request failed for {url}: {exc}")
        return None

    if response.status_code == 403:
        log(f"Received HTTP 403 for {url}; retrying after session bootstrap")
        if bootstrap_http_session():
            try:
                response = session.get(url, headers=headers, timeout=15)
            except Exception as exc:
                log(f"HTTP retry failed for {url}: {exc}")
                return None
    return response


def upload_to_r2(content, key, content_type="text/csv"):
    client = get_r2_client()
    if not client:
        return False
    try:
        client.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=content, ContentType=content_type)
        log(f"Uploaded to R2: {key}")
        return True
    except Exception as exc:
        log(f"Error uploading to R2: {exc}")
        return False


def get_from_r2(key):
    client = get_r2_client()
    if not client:
        return None
    try:
        response = client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        return response["Body"].read().decode("utf-8")
    except Exception:
        return None


def update_consolidated_file(new_df: pd.DataFrame, key: str):
    """Appends new data to a consolidated history file in R2."""
    existing_content = get_from_r2(key)
    if existing_content:
        existing_df = pd.read_csv(io.StringIO(existing_content))
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df

    upload_to_r2(combined_df.to_csv(index=False), key)


def load_dataframe_from_r2(key: str) -> pd.DataFrame | None:
    content = get_from_r2(key)
    if not content:
        return None
    return pd.read_csv(io.StringIO(content))


def maybe_sleep_between_requests():
    if DELAY_BETWEEN_REQUESTS > 0:
        time.sleep(DELAY_BETWEEN_REQUESTS)


def get_nested_table_primary_text(cell) -> str | None:
    nested_table = cell.find("table", recursive=False) or cell.find("table")
    if not nested_table:
        return None
    nested_row = nested_table.find("tr")
    if not nested_row:
        return ""
    nested_cells = nested_row.find_all("td", recursive=False)
    if not nested_cells:
        return ""
    return clean_text(nested_cells[0].get_text(" ", strip=True))


def extract_statewise_cell_text(cell, nested: bool = False) -> str:
    if nested:
        text = get_nested_table_primary_text(cell)
        if text is not None:
            return text
    return clean_text(cell.get_text(" ", strip=True))


def get_state_trend_table(soup: BeautifulSoup):
    return soup.select_one("div.custom-table table") or soup.select_one("div.table-responsive > table") or soup.find("table")


def get_state_trend_page_count(soup: BeautifulSoup, state_code: str) -> int | None:
    page_numbers = []
    for link in soup.select("a.page-link[href]"):
        href = link.get("href", "")
        match = re.search(rf"statewise{re.escape(state_code)}(\d+)\.htm", href)
        if match:
            page_numbers.append(int(match.group(1)))
    return max(page_numbers) if page_numbers else None


def build_partywise_snapshot(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []
    records = (
        df[["Party", "Won", "Leading", "Total"]]
        .fillna("")
        .astype(str)
        .sort_values(["Party", "Won", "Leading", "Total"], kind="stable")
        .to_dict("records")
    )
    return records


def extract_statewide_snapshot_from_rows(rows: list[dict]) -> dict:
    snapshot = {}
    for row in rows:
        constituency_key = row["Constituency_Key"]
        snapshot[constituency_key] = {
            "Constituency_No": str(row["Constituency_No"]),
            "Constituency_Name": str(row["Constituency_Name"]),
            "Leading_Candidate": str(row["Leading_Candidate"]),
            "Leading_Party": str(row["Leading_Party"]),
            "Trailing_Candidate": str(row["Trailing_Candidate"]),
            "Trailing_Party": str(row["Trailing_Party"]),
            "Margin": str(row["Margin"]),
            "Round": str(row.get("Round", "")),
            "Status": str(row["Status"]),
        }
    return snapshot


def get_changed_constituency_keys(previous_snapshot: dict, current_snapshot: dict) -> list[str]:
    changed_keys = []
    for constituency_key, current_row in current_snapshot.items():
        if previous_snapshot.get(constituency_key) != current_row:
            changed_keys.append(constituency_key)
    return changed_keys


def load_previous_party_snapshot(state_code: str, cache: dict) -> list[dict]:
    cached = cache["partywise"].get(state_code)
    if cached is not None:
        return cached

    current_key = get_state_dataset_keys(state_code, "partywise")["current"]
    df = load_dataframe_from_r2(current_key)
    if df is None:
        return []
    snapshot = build_partywise_snapshot(df)
    cache["partywise"][state_code] = snapshot
    return snapshot


def load_previous_statewide_snapshot(state_code: str, cache: dict) -> dict:
    cached = cache["statewide"].get(state_code)
    if cached is not None:
        return cached

    current_key = get_state_dataset_keys(state_code, "statewide-trends")["current"]
    df = load_dataframe_from_r2(current_key)
    if df is None:
        return {}
    snapshot = {}
    for row in df.fillna("").to_dict("records"):
        constituency_key = row.get("Constituency_Key") or make_constituency_key(
            state_code, row.get("Constituency_No", "")
        )
        snapshot[constituency_key] = {
            "Constituency_No": str(row.get("Constituency_No", "")),
            "Constituency_Name": str(row.get("Constituency_Name", "")),
            "Leading_Candidate": str(row.get("Leading_Candidate", "")),
            "Leading_Party": str(row.get("Leading_Party", "")),
            "Trailing_Candidate": str(row.get("Trailing_Candidate", "")),
            "Trailing_Party": str(row.get("Trailing_Party", "")),
            "Margin": str(row.get("Margin", "")),
            "Round": str(row.get("Round", "")),
            "Status": str(row.get("Status", "")),
        }

    cache["statewide"][state_code] = snapshot
    return snapshot


def build_summary_frames(
    state_codes: list[str], frames_by_state: dict[str, pd.DataFrame], dataset_name: str
) -> list[pd.DataFrame]:
    frames = []
    for state_code in state_codes:
        frame = frames_by_state.get(state_code)
        if frame is None:
            current_key = get_state_dataset_keys(state_code, dataset_name)["current"]
            frame = load_dataframe_from_r2(current_key)
        if frame is not None and not frame.empty:
            frames.append(frame)
    return frames


def fetch_party_wise(state_name: str, state_code: str) -> pd.DataFrame | None:
    """Fetches party-wise results for a state."""
    url = f"{get_election_base_url()}/partywiseresult-{state_code}.htm"
    timestamp = now_timestamp()
    try:
        response = fetch_url(url, referer=get_index_url())
        if response is None:
            return None
        if response.status_code != 200:
            log(f"Partywise {state_code} returned HTTP {response.status_code}: {url}")
            return None
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.select_one("div.rslt-table table") or soup.select_one("div.card-body table") or soup.find("table")
        if not table:
            return None

        rows = []
        tbody = table.find("tbody")
        if not tbody:
            return pd.DataFrame(rows)

        for tr in tbody.find_all("tr", recursive=False):
            tds = tr.find_all("td", recursive=False)
            if len(tds) < 4:
                continue
            rows.append(
                {
                    "Election_Id": ELECTION_ID,
                    "State": state_name,
                    "State_Code": state_code,
                    "Party": clean_text(tds[0].get_text(" ", strip=True)),
                    "Won": clean_text(tds[1].get_text(" ", strip=True)),
                    "Leading": clean_text(tds[2].get_text(" ", strip=True)),
                    "Total": clean_text(tds[3].get_text(" ", strip=True)),
                    "Timestamp": timestamp,
                }
            )
        return pd.DataFrame(rows)
    except Exception as exc:
        log(f"Error party-wise {state_code}: {exc}")
        return None


def fetch_state_trends(state_name: str, state_code: str, page_no: int) -> tuple[list[dict], int | None]:
    """Fetches state trend rows plus discovered pagination size."""
    url = f"{get_election_base_url()}/statewise{state_code}{page_no}.htm"
    timestamp = now_timestamp()
    results = []
    page_count = None
    try:
        response = fetch_url(url, referer=get_index_url())
        if response is None:
            return [], None
        if response.status_code != 200:
            log(f"Statewise {state_code} page {page_no} returned HTTP {response.status_code}: {url}")
            return [], None
        soup = BeautifulSoup(response.text, "html.parser")
        table = get_state_trend_table(soup)
        if not table:
            return [], None
        page_count = get_state_trend_page_count(soup, state_code)

        tbody = table.find("tbody") or table
        for tr in tbody.find_all("tr", recursive=False):
            tds = tr.find_all("td", recursive=False)
            if len(tds) < 9:
                continue

            ac_link = tds[0].find("a")
            href = ac_link.get("href", "") if ac_link else ""
            ac_match = re.search(r"(\d+)\.htm", href)
            if ac_match:
                constituency_no = int(ac_match.group(1))
            else:
                constituency_no_text = clean_text(tds[1].get_text(" ", strip=True))
                if not constituency_no_text.isdigit():
                    continue
                constituency_no = int(constituency_no_text)

            results.append(
                {
                    "Election_Id": ELECTION_ID,
                    "State": state_name,
                    "State_Code": state_code,
                    "Constituency_No": constituency_no,
                    "Constituency_Key": make_constituency_key(state_code, constituency_no),
                    "Constituency_Name": extract_statewise_cell_text(tds[0]),
                    "Leading_Candidate": extract_statewise_cell_text(tds[2]),
                    "Leading_Party": extract_statewise_cell_text(tds[3], nested=True),
                    "Trailing_Candidate": extract_statewise_cell_text(tds[4]),
                    "Trailing_Party": extract_statewise_cell_text(tds[5], nested=True),
                    "Margin": extract_statewise_cell_text(tds[6]),
                    "Round": extract_statewise_cell_text(tds[7]),
                    "Status": extract_statewise_cell_text(tds[8]),
                    "Timestamp": timestamp,
                }
            )
    except Exception as exc:
        log(f"Error trends {state_name} P{page_no}: {exc}")
    return results, page_count


def fetch_constituency_details(
    state_name: str, state_code: str, constituency_no: int, constituency_name: str
) -> pd.DataFrame | None:
    """Fetches candidate-wise details for a constituency."""
    url = f"{get_election_base_url()}/Constituencywise{state_code}{constituency_no}.htm"
    timestamp = now_timestamp()
    try:
        statewise_referer = f"{get_election_base_url()}/statewise{state_code}1.htm"
        response = fetch_url(url, referer=statewise_referer)
        if response is None:
            return None
        if response.status_code != 200:
            log(f"Constituency {state_code}-{constituency_no} returned HTTP {response.status_code}: {url}")
            return None
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.select_one("div.table-responsive > table") or soup.find("table")
        if not table:
            return None

        rows = []
        tbody = table.find("tbody") or table
        for tr in tbody.find_all("tr"):
            tds = tr.find_all("td", recursive=False)
            if len(tds) < 6:
                continue
            rows.append(
                {
                    "Election_Id": ELECTION_ID,
                    "State": state_name,
                    "State_Code": state_code,
                    "Constituency_No": constituency_no,
                    "Constituency_Key": make_constituency_key(state_code, constituency_no),
                    "Constituency_Name": constituency_name,
                    "SN": tds[0].text.strip(),
                    "Candidate": tds[1].text.strip(),
                    "Party": tds[2].text.strip(),
                    "EVM_Votes": tds[3].text.strip(),
                    "Postal_Votes": tds[4].text.strip(),
                    "Total_Votes": tds[5].text.strip(),
                    "Vote_Percentage": tds[6].text.strip() if len(tds) > 6 else "",
                    "Timestamp": timestamp,
                }
            )
        return pd.DataFrame(rows) if rows else None
    except Exception as exc:
        log(f"Error detail {state_name} AC {constituency_no}: {exc}")
        return None


def load_cache() -> dict:
    cache_file = get_cache_file()
    if not cache_file.exists():
        return ensure_cache_shape({})
    try:
        with cache_file.open("r", encoding="utf-8") as handle:
            return ensure_cache_shape(json.load(handle))
    except Exception:
        return ensure_cache_shape({})


def save_cache(cache: dict):
    cache_file = get_cache_file()
    with cache_file.open("w", encoding="utf-8") as handle:
        json.dump(cache, handle, indent=2)


def scrape_all():
    log(f"Cycle starting: {datetime.now()}")
    bootstrap_http_session()
    cache = load_cache()
    state_codes = [config["code"] for config in STATE_CONFIG.values()]
    state_trend_frames_by_state = {}
    party_frames_by_state = {}
    any_statewide_change = False
    any_partywise_change = False

    for state_name, config in STATE_CONFIG.items():
        state_code = config["code"]
        log(f"Starting state scrape: {state_name} ({state_code})")

        party_df = fetch_party_wise(state_name, state_code)
        if party_df is not None:
            party_frames_by_state[state_code] = party_df
            previous_party_snapshot = load_previous_party_snapshot(state_code, cache)
            current_party_snapshot = build_partywise_snapshot(party_df)
            log(f"{state_code} partywise rows fetched: {len(party_df)}")

            if current_party_snapshot != previous_party_snapshot:
                party_keys = get_state_dataset_keys(state_code, "partywise")
                upload_to_r2(party_df.to_csv(index=False), party_keys["current"])
                update_consolidated_file(party_df, party_keys["history"])
                cache["partywise"][state_code] = current_party_snapshot
                any_partywise_change = True
                log(f"{state_code} partywise snapshot changed")
            else:
                log(f"{state_code} partywise snapshot unchanged")
        else:
            log(f"{state_code} partywise fetch returned no table")

        state_trends = []
        max_trend_pages = config["trend_pages"]
        page_no = 1
        while page_no <= max_trend_pages:
            page_results, discovered_trend_pages = fetch_state_trends(state_name, state_code, page_no)
            if discovered_trend_pages:
                max_trend_pages = discovered_trend_pages
            log(
                f"{state_code} statewise page {page_no}/{max_trend_pages} rows: {len(page_results)}"
            )
            if not page_results:
                break
            state_trends.extend(page_results)
            maybe_sleep_between_requests()
            page_no += 1

        if not state_trends:
            log(f"{state_code} produced no statewide rows")
            continue

        trends_df = pd.DataFrame(state_trends)
        state_trend_frames_by_state[state_code] = trends_df
        log(f"{state_code} total statewide rows fetched: {len(trends_df)}")

        previous_statewide_snapshot = load_previous_statewide_snapshot(state_code, cache)
        current_statewide_snapshot = extract_statewide_snapshot_from_rows(state_trends)
        changed_constituency_keys = get_changed_constituency_keys(
            previous_statewide_snapshot, current_statewide_snapshot
        )
        log(f"{state_code} changed constituencies: {len(changed_constituency_keys)}")

        if changed_constituency_keys:
            state_trend_keys = get_state_dataset_keys(state_code, "statewide-trends")
            upload_to_r2(trends_df.to_csv(index=False), state_trend_keys["current"])
            update_consolidated_file(trends_df, state_trend_keys["history"])
            cache["statewide"][state_code] = current_statewide_snapshot
            any_statewide_change = True
            log(f"{state_code} statewide snapshot changed")
        else:
            log(f"{state_code} statewide snapshot unchanged")

        if changed_constituency_keys:
            changed_constituencies = [trend for trend in state_trends if trend["Constituency_Key"] in changed_constituency_keys]
            for trend in changed_constituencies:
                log(f"Update: {state_name} AC {trend['Constituency_No']}")
                detail_df = fetch_constituency_details(
                    state_name,
                    trend["State_Code"],
                    trend["Constituency_No"],
                    trend["Constituency_Name"],
                )
                if detail_df is not None:
                    candidate_keys = get_constituency_dataset_keys(
                        trend["State_Code"], trend["Constituency_No"], "candidatewise"
                    )
                    upload_to_r2(detail_df.to_csv(index=False), candidate_keys["current"])
                    update_consolidated_file(detail_df, candidate_keys["history"])
                    log(
                        f"{trend['State_Code']}-{trend['Constituency_No']} candidate rows fetched: {len(detail_df)}"
                    )
                else:
                    log(f"{trend['State_Code']}-{trend['Constituency_No']} candidate fetch returned no table")
                maybe_sleep_between_requests()

    if any_statewide_change:
        summary_state_frames = build_summary_frames(
            state_codes, state_trend_frames_by_state, "statewide-trends"
        )
        if summary_state_frames:
            global_trends_df = pd.concat(summary_state_frames, ignore_index=True)
            summary_statewide = get_summary_dataset_keys("statewide-trends")
            upload_to_r2(global_trends_df.to_csv(index=False), summary_statewide["current"])
            update_consolidated_file(global_trends_df, summary_statewide["history"])

    if any_partywise_change:
        summary_party_frames = build_summary_frames(state_codes, party_frames_by_state, "partywise")
        if summary_party_frames:
            global_party_df = pd.concat(summary_party_frames, ignore_index=True)
            summary_partywise = get_summary_dataset_keys("partywise")
            upload_to_r2(global_party_df.to_csv(index=False), summary_partywise["current"])
            update_consolidated_file(global_party_df, summary_partywise["history"])

    upload_to_r2(json.dumps(build_manifest(), indent=2), get_manifest_key(), "application/json")
    save_cache(cache)
    log(f"Cycle complete: {datetime.now()}")


def main():
    while True:
        cycle_started_at = time.time()
        update_config()
        try:
            scrape_all()
        except Exception as exc:
            log(f"Error during scrape: {exc}")

        elapsed_seconds = time.time() - cycle_started_at
        sleep_seconds = max(0.0, POLL_INTERVAL_SECONDS - elapsed_seconds)
        log(f"[{datetime.now()}] Cycle complete. Sleeping for {sleep_seconds:.1f} seconds...")
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
