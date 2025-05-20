"""Configuration module for ClickHouse connection and LLM model settings."""

# --- Import necessary libraries ---
import os
from dotenv import load_dotenv

# load .env from your repo root
load_dotenv()

# --- LLM model constants ---
MODEL_GEMINI_2_0_FLASH = "gemini-2.0-flash"
MODEL_GPT_4O = "gpt-4.1"
MODEL_CLAUDE_SONNET = "claude-3-5-sonnet-latest"

# --- ClickHouse connection pieces from .env ---
CLICKHOUSE = {
    "protocol": os.getenv("CLICKHOUSE_PROTOCOL", "https"),
    "host": os.getenv("CLICKHOUSE_HOST"),
    "port": int(os.getenv("CLICKHOUSE_PORT", "8443")),
    "user": os.getenv("CLICKHOUSE_USER"),
    "password": os.getenv("CLICKHOUSE_PASS"),
    "database": os.getenv("CLICKHOUSE_DB", "default"),
}

# optional: full URL if you need it anywhere
CLICKHOUSE_URL = (
    f"{CLICKHOUSE['protocol']}://"
    f"{CLICKHOUSE['user']}:{CLICKHOUSE['password']}@"
    f"{CLICKHOUSE['host']}:{CLICKHOUSE['port']}/"
    f"{CLICKHOUSE['database']}"
)
