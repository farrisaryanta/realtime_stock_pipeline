import json
import pandas as pd
import boto3
from io import BytesIO
from kafka import KafkaConsumer
from datetime import datetime
from config import KAFKA_BROKER, AWS_SECRET_KEY, S3_BUCKET_NAME
from pipeline_log import get_logger

# Initialize custom logger
logger = get_logger(__name__)

# 1. Initialize Kafka Consumer
consumer = KafkaConsumer(
    'raw_stock_trades',
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='latest', # Start reading the newest messages
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Decode bytes back to Python dictionary
)

# 2. Initialize AWS S3 Client (Local MinIO)
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000', # Points to your local MinIO server
    aws_access_key_id='minioadmin',       # Default MinIO user
    aws_secret_access_key='minioadmin',   # Default MinIO password
    region_name='us-east-1'               # Region is requried by boto3, but ignored by MinIO
)

def process_batch(messages):
    """
    Transforms a list of raw trade messages ito an OHLC (Open, High, Low, Close)
    aggregated DataFrame and uploads it to AWS S3 in Parquet format.
    """
    if not messages:
        return

    # Convert the list of raw dictionaries into a Pandas DataFrame
    df = pd.DataFrame(messages)

    # Finnhub JSON structure uses short keys: 'p' (price), 's' (symbol), 't' (timestamp), 'v' (volume)
    # Rename columns for better readability and downstream analytics
    df = df.rename(columns={'p': 'price', 's': 'symbol', 't': 'timestamp', 'v': 'volume'})

    # Convert unix timestamp (milliseconds) to standard datetime format
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')

    # Aggregate data to calculate teh OHLC and total volume per symbol for this specific batch
    aggregated_df = df.groupby('symbol').agg(
        open_price=('price', 'first'),
        high_price=('price', 'max'),
        low_price=('price', 'min'),
        close_price=('price', 'last'),
        total_volume=('volume', 'sum')
    ).reset_index()

    # Add a column to track when this data was processed
    current_time = datetime.now()
    aggregated_df['processed_at'] = current_time

    # Convert the aggregated DataFrame to Parquet format in a memory buffer
    parquet_buffer = BytesIO()
    aggregated_df.to_parquet(parquet_buffer, index=False)

    # Define the S3 file path.
    date_str = current_time.strftime("%Y-%m-%d")
    hour_str = current_time.strftime("%H")
    file_name = f"processed_data/date={date_str}/hour={hour_str}/ohlc_{current_time.strftime('%M%S')}.parquet"

    # Upload the buffer to AWS S3
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=file_name,
            Body=parquet_buffer.getvalue()
        )
        logger.info(f"Successfully uploaded {file_name} to AWS S3 Data Lake.")
    except Exception as e:
        logger.error(f"Failed to upload to S3: {e}")

def run_transformer():
    """Reads messages continuously from Kafka and processes the in micro-batches."""
    logger.info("Starting Transform module, Listening to Kafka broker...")

    batch = []
    # Micro-batch size: process data every time we collect 100 trades.
    BATCH_SIZE = 100

    for message in consumer:
        trade_data = message.value
        batch.append(trade_data)

        # Once the batch is full, process it and clear the list for the next batch
        if len(batch) >= BATCH_SIZE:
            process_batch(batch)
            batch = []

if __name__ == "__main__":
    run_transformer()