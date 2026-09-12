import time
import boto3
import pandas as pd
from io import BytesIO
from sqlalchemy import create_engine
from config import DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME, S3_BUCKET_NAME
from pipeline_log import get_logger

# Initialize custom logger
logger = get_logger(__name__)

# 1. Initialize AWS S3 Client (Local MinIO)
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000', # Points to your local MinIO server
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    region_name='us-east-1'
)

# 2. Initialize PostgreSQL Engine using SQLAlchemy
DATABASE_URI = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
try:
    engine = create_engine(DATABASE_URI)
    logger.info("Successfully connected to PostgreSQL database.")
except Exception as e:
    logger.error(f"Failed to connect to PostgreSQL: {e}")
    exit(1)

def load_data_to_db():
    """
    Scans the S3 bucket for new Parquet files, loads them into Pandas,
    inserts the data into PostgreSQL, and deletes the processed files.
    """
    try:
        # List all objects in the processed_data folder
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix='processed_data/')
        
        if 'Contents' not in response:
            return # No new files found
            
        for obj in response['Contents']:
            file_key = obj['Key']
            
            # Skip if it's not a parquet file
            if not file_key.endswith('.parquet'):
                continue
                
            logger.info(f"Found new file: {file_key}. Starting load process...")
            
            # 1. Read file from S3 into memory
            s3_object = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
            parquet_bytes = BytesIO(s3_object['Body'].read())
            
            # 2. Convert to Pandas DataFrame
            df = pd.read_parquet(parquet_bytes)
            
            # 3. Load to PostgreSQL
            df.to_sql('realtime_stock_summary', engine, if_exists='append', index=False)
            logger.info(f"Successfully loaded {len(df)} rows into PostgreSQL.")
            
            # 4. Delete the file from S3 to prevent duplicate loading
            s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=file_key)
            logger.info(f"Deleted {file_key} from S3 bucket to prevent duplication.")
            
    except Exception as e:
        logger.error(f"Error during load process: {e}")

def run_loader():
    """Continuously polls the S3 Data Lake for new files."""
    logger.info("Starting Load module. Polling S3 for new data every 60 seconds...")
    
    while True:
        load_data_to_db()
        # Pause for 60 seconds before checking S3 again to save CPU resources
        time.sleep(60)

if __name__ == "__main__":
    run_loader()