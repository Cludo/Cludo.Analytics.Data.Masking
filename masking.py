import logging
from datetime import datetime, timedelta
import requests
from clickhouse_driver import Client
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# ─── CONFIGURATION ─────────────────────────────────────────────────────────────
# General parameters
CUSTOMER_ID = int(os.getenv("CUSTOMER_ID"))
ENGINE_ID = int(os.getenv("ENGINE_ID"))
START_DATETIME = os.getenv("START_DATETIME")
END_DATETIME = os.getenv("END_DATETIME")
INTERVAL_MINUTES = int(os.getenv("INTERVAL_MINUTES"))
MODE = os.getenv("MODE")

# ClickHouse connection
CLICKHOUSE_CONFIG = {
    "host": os.getenv("CLICKHOUSE_HOST"),
    "port": int(os.getenv("CLICKHOUSE_PORT")),
    "database": os.getenv("CLICKHOUSE_DATABASE"),
    "send_receive_timeout": int(os.getenv("CLICKHOUSE_TIMEOUT")),
}

# Authentication
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")

# Tables and columns to mask
TABLES_TO_MASK = {
    "chat_events": {
        "columns": {
            "ip_address": "'***'",
            "session_id": "'***'",
            "conversation_id": "'00000000-0000-0000-0000-000000000000'",
            "exchange_id": "'00000000-0000-0000-0000-000000000000'",
            "text": "'***'",
            "text_raw": "'***'",
            "device_id": "'00000000-0000-0000-0000-000000000000'",
            "device_type": "'*'",
            "feedback_reason": "'***'",
            "geo_continent_name": "'***'",
            "geo_country_iso_code": "'***'",
            "geo_country_name": "'***'",
            "geo_region_name": "'***'",
            "geo_city_name": "'***'",
            "geo_latitude": 0.0000,
            "geo_longitude": 0.0000,
            "browser_language": "'***'",
        }
    },
    "clicks_per_month": {
        "columns": {
            "query": "'***'",
            "url": "'***'"
        }
    },
    "chat_exchanges": {
        "columns": {
            "ip_address": "'***'",
            "device_type": "'*'",
            "geo_continent_name": "'***'",
            "geo_country_iso_code": "'***'",
            "geo_country_name": "'***'",
            "geo_region_name": "'***'",
            "geo_city_name": "'***'",
            "geo_latitude": 0,
            "geo_longitude": 0,
            "question": "'***'",
            "answer": "'***'",
            "feedback_reason": "'***'",
            "feedback_comment": "'***'",
            "error_message": "'***'"
        }
    },
    "chat_conversations": {
        "columns": {
            "ip_address": "'***'",
            "device_type": "'*'",
            "geo_continent_name": "'***'",
            "geo_country_iso_code": "'***'",
            "geo_country_name": "'***'",
            "geo_region_name": "'***'",
            "geo_city_name": "'***'",
            "geo_latitude": 0,
            "geo_longitude": 0,
            "original_question": "'***'",
            "questions": 0,
            "answers": 0,
            "feedback_ratings": 0,
            "feedback_reasons": 0,
            "feedback_comments": 0
        }
    },
    "search_summary_events": {
        "columns": {
            "text": "'***'",
            "text_raw": "'***'",
            "feedback_reason": "'***'",
            "ip_address": "'***'",
            "session_id": "'***'",
            "device_id": "'***'",
            "user_agent": "'***'",
            "geo_city_name": "'***'",
            "geo_latitude": 0.0000,
            "geo_longitude": 0.0000
        }
    },
    "search_summaries": {
        "columns": {
            "ip_address": "'***'",
            "geo_city_name": "'***'",
            "geo_latitude": 0.0000,
            "geo_longitude": 0.0000,
            "query": "'***'",
            "summary": "'***'",
            "feedback_reason": "'***'",
            "feedback_comment": "'***'",
            "error_message": "'***'"
        }
    },
    "events": {
        "columns": {
            "ip_address": "'***'",
            "device_id": "'*'",
            "session_id": "'***'",
            "geo_city_name": "'***'",
            "geo_latitude": 0,
            "geo_longitude": 0,
            "user_agent": "'***'",
            "query": "'***'",
            "referrer_url": "'***'",
            "referrer_title": "'***'",
            "traits": "['***']"
        }
    }
}
# ───────────────────────────────────────────────────────────────────────────────

# Setup logging
logging.basicConfig(
    filename="ClickhouseMasking.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w",
)
console = logging.StreamHandler()
console.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
logging.getLogger().addHandler(console)


def parse_intervals(start_str, end_str, interval_minutes):
    start = datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S")
    end = datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S")
    intervals = []

    while start < end:
        next_point = min(start + timedelta(minutes=interval_minutes), end)
        intervals.append((start, next_point))
        start = next_point

    return intervals


def build_update_query(table, columns, time_start, time_end):
    # Use 'first' as the date field for specific tables
    date_field = "first" if table in ["chat_exchanges", "chat_conversations", "search_summaries"] else "date"

    sets = ",\n    ".join(f"{col} = {mask}" for col, mask in columns.items())
    conditions = [
        f"customer_id = {CUSTOMER_ID}",
        f"{date_field} >= toDateTime('{time_start}')",
        f"{date_field} < toDateTime('{time_end}')"
    ]
    if ENGINE_ID is not None:
        conditions.append(f"engine_id = {ENGINE_ID}")

    return f"""ALTER TABLE {CLICKHOUSE_CONFIG['database']}.{table}_local ON CLUSTER analytics
UPDATE
    {sets}
WHERE
    {" AND ".join(conditions)};"""


def execute_query_via_http(query):
    # Construct the full URL with host and port
    url = f"{CLICKHOUSE_CONFIG['host']}:{CLICKHOUSE_CONFIG['port']}"
    
    response = requests.post(
        url,
        data=query,
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),  # Use credentials from .env
    )
    if response.status_code == 200:
        logging.info("Query executed successfully.")
    else:
        logging.error(f"Query failed: {response.text}")
        raise Exception(f"Query failed: {response.text}")


def main():
    client = None
    if MODE == "EXECUTE":
        logging.info("Connecting to ClickHouse...")
        client = Client(**CLICKHOUSE_CONFIG)

    intervals = parse_intervals(START_DATETIME, END_DATETIME, INTERVAL_MINUTES)
    queries = []

    for table, config in TABLES_TO_MASK.items():
        columns = config["columns"]
        for start, end in intervals:
            query = build_update_query(table, columns, start, end)
            queries.append(query)

            if MODE == "EXECUTE":
                logging.info(f"Running query on table '{table}' from {start} to {end}")
                try:
                    execute_query_via_http(query)
                    logging.info("Query executed successfully.")
                except Exception as e:
                    logging.error(f"Query failed: {e}")

    if MODE == "GENERATE":
        file_name = f"queries_{CUSTOMER_ID}_{ENGINE_ID or 'all'}.sql"
        with open(file_name, "w") as f:
            f.write("\n\n".join(queries))
        logging.info(f"Generated queries saved to: {file_name}")


if __name__ == "__main__":
    main()
