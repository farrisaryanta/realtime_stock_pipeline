import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from a .env file (if it exists)
load_dotenv()

# Define the base directory of the project
BASE_DIR = Path(__file__).resolve().parent

# API & Message Broker Settings
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "YOUR_FINNHUB_API_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

# Storage Settings (AWS S3 / MinIO)
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY", "minioadmin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY", "minioadmin")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "stock-datalake")

# Database Settings (PostgreSQL)
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "YOUR_DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME", "stock_analytics")

# Logging Settings
LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "pipeline.log"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")