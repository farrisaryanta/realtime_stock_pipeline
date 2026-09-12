from config import config
from pipeline_log import setup_logger

# Initialize the logger for this specific script
logger = setup_logger(__name__)

def connect_to_database():
    """
    Simulates a database connection using variables from config.py.
    """
    logger.info("Attempting to connect to the database...")
    
    try:
        # Example of using the config object
        host = config.DB_HOST
        db_name = config.DB_NAME
        
        logger.debug(f"Connection parameters: Host={host}, Database={db_name}")
        
        # Simulating connection success
        logger.info("Successfully connected to the database.")
        return True
        
    except Exception as e:
        logger.error(f"Failed to connect to the database: {e}")
        return False

def run_pipeline():
    """
    Main function to execute the data pipeline.
    """
    logger.info(f"Starting {config.PROJECT_NAME} in {config.ENVIRONMENT} environment.")
    
    is_connected = connect_to_database()
    
    if is_connected:
        logger.info("Data extraction started.")
        # Your pipeline logic here...
        logger.info("Pipeline execution finished successfully.")
    else:
        logger.critical("Pipeline execution aborted due to database connection failure.")

if __name__ == "__main__":
    run_pipeline()