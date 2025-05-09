# Analytics Data Masking Tool

This project provides a Python script to mask sensitive data in ClickHouse tables. The script can either execute masking queries directly on the database or generate SQL queries for manual execution.

---

## Features

- Masks sensitive data in specified ClickHouse tables.
- Supports execution of masking queries or generation of SQL files.
- Configurable time intervals for processing data.
- Logs all operations to a file (`ClickhouseMasking.log`) and console.

---

## Prerequisites

1. **Python**: Ensure Python 3.7 or higher is installed.
2. **Dependencies**: Install the required Python libraries:
   ```bash
   pip install requests clickhouse-driver python-dotenv
   ```

---

## Environmental Variables

The script uses a `.env` file to configure its parameters. Below are the required variables:

### General Parameters
- `CUSTOMER_ID`: The customer ID (integer).
- `ENGINE_ID`: The engine ID (integer).
- `START_DATETIME`: The start datetime for processing data (format: `YYYY-MM-DD HH:MM:SS`).
- `END_DATETIME`: The end datetime for processing data (format: `YYYY-MM-DD HH:MM:SS`).
- `INTERVAL_MINUTES`: The interval in minutes for processing data (integer).
- `MODE`: The mode of operation. Options:
  - `EXECUTE`: Executes the masking queries directly on the database.
  - `GENERATE`: Generates SQL queries and saves them to a file.

### ClickHouse Connection
- `CLICKHOUSE_HOST`: The ClickHouse server host (e.g., `http://clickhouse.example.com`).
- `CLICKHOUSE_PORT`: The ClickHouse server port (integer).
- `CLICKHOUSE_DATABASE`: The ClickHouse database name.
- `CLICKHOUSE_TIMEOUT`: The timeout for ClickHouse operations (integer, in seconds).
- `CLICKHOUSE_USER`: The username for ClickHouse authentication.
- `CLICKHOUSE_PASSWORD`: The password for ClickHouse authentication.

Example `.env` file:
```properties
CUSTOMER_ID=xxx
ENGINE_ID=xxx
START_DATETIME=2025-04-08 00:00:00
END_DATETIME=2025-05-08 00:00:00
INTERVAL_MINUTES=1440
MODE=EXECUTE

CLICKHOUSE_HOST=http://clickhouse.eu1.cludo.com
CLICKHOUSE_PORT=8123
CLICKHOUSE_DATABASE=analytics
CLICKHOUSE_TIMEOUT=60
CLICKHOUSE_USER=xxx
CLICKHOUSE_PASSWORD=xxx
```

---

## Tables and Columns Affected by Masking

The script applies masking to the following tables and columns:

1. **`chat_events`**
   - Columns: `ip_address`, `session_id`, `conversation_id`, `exchange_id`, `text`, `text_raw`, `device_id`, `device_type`, `feedback_reason`, `geo_*` (continent, country, region, city), `geo_latitude`, `geo_longitude`, `browser_language`.

2. **`chat_exchanges`**
   - Columns: `ip_address`, `device_type`, `geo_*` (continent, country, region, city), `geo_latitude`, `geo_longitude`, `question`, `answer`, `feedback_reason`, `feedback_comment`, `error_message`.

3. **`chat_conversations`**
   - Columns: `ip_address`, `device_type`, `geo_*` (continent, country, region, city), `geo_latitude`, `geo_longitude`, `original_question`, `questions`, `answers`, `feedback_ratings`, `feedback_reasons`, `feedback_comments`.

4. **`search_summary_events`**
   - Columns: `text`, `text_raw`, `feedback_reason`, `ip_address`, `session_id`, `device_id`, `user_agent`, `geo_city_name`, `geo_latitude`, `geo_longitude`.

5. **`search_summaries`**
   - Columns: `ip_address`, `geo_city_name`, `geo_latitude`, `geo_longitude`, `query`, `summary`, `feedback_reason`, `feedback_comment`, `error_message`.

6. **`events`**
   - Columns: `ip_address`, `device_id`, `session_id`, `geo_city_name`, `geo_latitude`, `geo_longitude`, `user_agent`, `query`, `referrer_url`, `referrer_title`, `traits`.

---

## How to Run the Code

1. Clone the repository and navigate to the project directory:
   ```bash
   git clone https://github.com/your-repo/analytics-data-masking.git
   cd analytics-data-masking
   ```

2. Create a `.env` file in the project directory and configure the required variables as described above.

3. Install the required dependencies:
   ```bash
   pip install requests clickhouse-driver python-dotenv
   ```

4. Run the script:
   ```bash
   python masking.py
   ```

   - If `MODE=EXECUTE`, the script will execute the masking queries directly on the database.
   - If `MODE=GENERATE`, the script will generate SQL queries and save them to a file.

5. Check the logs for details:
   - Console output.
   - Log file: `ClickhouseMasking.log`.

---

## Notes

- Ensure the ClickHouse credentials and connection details in the `.env` file are correct.
- If using `GENERATE` mode, the generated SQL file will be saved in the project directory with a name like `queries_<CUSTOMER_ID>_<ENGINE_ID>.sql`.

## WARNING: 
If using `EXECUTE` with production, please make sure that the CUSTOMER_ID is correct. ACTIONS ARE IRREVERSIBLE.