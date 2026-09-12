import json
import websocket
from kafka import KafkaProducer
from config import FINNHUB_API_KEY, KAFKA_BROKER
from pipeline_log import get_logger

# Initialize custom logger
logger = get_logger(__name__)

# Initialize Kafka Producer
# value_serializer ensures the dictionary is converted to a JSON string encoded in UTF-8 before sending
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info("Successfully connected to Kafka Broker.")
except Exception as e:
    logger.info(f"Failed to connect to Kafka Broker: {e}")
    exit(1)

KAFKA_TOPIC = 'raw_stock_trades'
TARGET_SYMBOLS = ["AAPL", "BINANCE:BTCUSDT", "AMZN"]

def on_message(ws, message):
    """
    Callback executed when a message is received from the WebSocket.
    Parses the JSON data and sends it to the Kafka topic.
    """
    try:
        data = json.loads(message)

        # Finnhub sends trade data under the 'data' key with a type of 'trade'
        if data.get('type') == 'trade':
            for trade in data['data']:
                # Send individual trade records to Kafka
                producer.send(KAFKA_TOPIC, trade)

            # Print to console for debugging purposes during development
            logger.info(f"Published {len(data['data'])} trades to Kafka topic '{KAFKA_TOPIC}'")

    except Exception as e:
        logger.error(f"Error processing message: {e}")

def on_error(ws, error):
    """Callback executed when a WebScoket error occurs."""
    logger.error(f"WebScoket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    """Callback executed when a WebSocket connection is closed."""
    logger.warning("WebSocket connection closed.")
    # Ensure all pending messages are sent to Kafka before closing
    producer.flush()

def on_open(ws):
    """
    Callback executed when the WebSocket connection is established.
    Subcribes to the target stock symbols.
    """
    logger.info("WebSocket connection opened. Subscribing to symbols...")
    for symbol in TARGET_SYMBOLS:
        subscribe_msg = {"type": "subscribe", "symbol": symbol}
        ws.send(json.dumps(subscribe_msg))
        logger.info(f"Subscribed to {symbol}")

def run_extractor():
    """Main function to initialize and run the WebSocket client."""
    websocket_url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"

    # Enable trace for detailed WebSocket debugging (set to False in production)
    websocket.enableTrace(False)

    ws = websocket.WebSocketApp(
        websocket_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )

    # Run the connection indefinitely
    ws.run_forever()

if __name__ == "__main__":
    logger.info("Starting Data Extraction Pipeline...")
    run_extractor()