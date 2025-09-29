
import faust
import json
import logging
from collections import deque
from scipy.stats import zscore
import numpy as np
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Faust App setup
# Read broker from env (Docker sets KAFKA_BROKER_URL=kafka:9092). Default to localhost:9094 for local.
broker_url = os.getenv('KAFKA_BROKER_URL', 'localhost:9094')
app = faust.App(
    'herd-behavior-alerter',
    broker=f'kafka://{broker_url}',
    store='rocksdb://',
    value_serializer='raw',
    web_port=6066, # For Faust web UI
)

# Kafka Topic for incoming user events
user_events_topic = app.topic('user_events')

# Kafka Topic for outgoing trending product alerts
trending_products_topic = app.topic('trending_products')

# In-memory table to store product activity (e.g., counts over a window)
# Key: product_id, Value: {count: int, last_updated: datetime, activity_window: deque}
product_activity_table = app.Table(
    'product_activity',
    key_type=str,
    value_type=dict,
    default=dict,
    partitions=1,
)

# Configuration for anomaly detection
WINDOW_SIZE = 60 # Number of data points (e.g., 1-minute intervals) for moving average/std dev
ZSCORE_THRESHOLD = 3.0 # Number of standard deviations above baseline to flag as anomaly
ACTIVITY_INTERVAL_SECONDS = 60 # How often to aggregate activity (e.g., every minute)

@app.agent(user_events_topic)
async def process_user_events(events):
    logger.info("Starting user event processing agent...")
    async for event_bytes in events:
        try:
            event = json.loads(event_bytes.decode('utf-8'))
            event_type = event.get('event_type')
            product_id = event.get('product_id')

            if product_id and event_type in ['view_product', 'add_to_cart']:
                async with product_activity_table.proxy(product_id) as product_data:
                    if 'activity_window' not in product_data:
                        product_data['activity_window'] = deque(maxlen=WINDOW_SIZE)
                    if 'current_interval_count' not in product_data:
                        product_data['current_interval_count'] = 0
                    if 'last_interval_start' not in product_data:
                        product_data['last_interval_start'] = app.current_time().timestamp()

                    product_data['current_interval_count'] += 1
                    product_data['last_updated'] = app.current_time().isoformat()
                    logger.debug(f"Event for product {product_id}: {event_type}. Current count: {product_data['current_interval_count']}")

            else:
                logger.debug(f"Skipping event: {event_type} for product {product_id}")

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON event: {event_bytes}. Error: {e}")
        except Exception as e:
            logger.error(f"Error processing event: {e}", exc_info=True)

@app.timer(interval=ACTIVITY_INTERVAL_SECONDS)
async def aggregate_and_detect_anomalies():
    logger.info("Running anomaly detection and aggregation...")
    for product_id, product_data in product_activity_table.items():
        if not product_data or 'activity_window' not in product_data:
            continue

        current_interval_count = product_data.get('current_interval_count', 0)
        activity_window = product_data['activity_window']

        # Add current interval count to the window and reset for next interval
        activity_window.append(current_interval_count)
        product_data['current_interval_count'] = 0
        product_data['last_interval_start'] = app.current_time().timestamp()

        if len(activity_window) < WINDOW_SIZE:
            logger.debug(f"Product {product_id}: Not enough data for anomaly detection (need {WINDOW_SIZE}, have {len(activity_window)})")
            continue

        # Convert deque to numpy array for calculations
        activity_array = np.array(list(activity_window))

        # Calculate moving average and standard deviation
        mean_activity = np.mean(activity_array[:-1]) # Exclude current interval for baseline
        std_dev_activity = np.std(activity_array[:-1]) # Exclude current interval for baseline

        # Anomaly detection using Z-score
        if std_dev_activity == 0:
            # If std dev is 0, all previous activities were the same. Anomaly if current is different.
            is_anomaly = current_interval_count > mean_activity and current_interval_count > 0
            z_score = float('inf') if is_anomaly else 0.0
        else:
            z_score = (current_interval_count - mean_activity) / std_dev_activity
            is_anomaly = z_score > ZSCORE_THRESHOLD

        logger.info(f"Product {product_id}: Current activity={current_interval_count}, Mean={mean_activity:.2f}, StdDev={std_dev_activity:.2f}, Z-score={z_score:.2f}, Anomaly={is_anomaly}")

        if is_anomaly:
            alert_message = {
                'product_id': product_id,
                'status': 'trending',
                'current_activity': current_interval_count,
                'baseline_mean': mean_activity,
                'baseline_std_dev': std_dev_activity,
                'z_score': z_score,
                'timestamp': app.current_time().isoformat(),
                'message': f"Product {product_id} is showing anomalous activity! Z-score: {z_score:.2f}"
            }
            await trending_products_topic.send(key=product_id, value=json.dumps(alert_message).encode('utf-8'))
            logger.warning(f"TRENDING ALERT: {alert_message['message']}")

if __name__ == '__main__':
    app.main()
