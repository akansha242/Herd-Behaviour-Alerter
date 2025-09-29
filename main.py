import json
import os
import logging
from typing import List, Dict, Any
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import asyncio
from herd_behavior_alerter.backend.api import router as api_router, update_trending_products_cache, add_to_recent_alerts
from herd_behavior_alerter.backend.alerting import alerting_service

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Herd Behavior Alerter Backend",
    description="API for ingesting user events and serving real-time trending product alerts."
)

# CORS middleware to allow frontend to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust this to your frontend origin in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# Include API routes
app.include_router(api_router)

# Kafka Producer setup
kafka_producer = None
# Prefer env var when running in Docker; fall back to localhost for local dev
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "localhost:9094")
USER_EVENTS_TOPIC = "user_events"
TRENDING_PRODUCTS_TOPIC = "trending_products"

def get_kafka_producer():
    global kafka_producer
    if kafka_producer is None:
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER_URL],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks='all'
            )

            logger.info(f"Kafka producer connected to {KAFKA_BROKER_URL}")
        except KafkaError as e:
            logger.error(f"Could not connect to Kafka broker: {e}")
            kafka_producer = None
    return kafka_producer

@app.on_event("startup")
async def startup_event():
    get_kafka_producer()

@app.on_event("shutdown")
async def shutdown_event():
    if kafka_producer:
        kafka_producer.close()
        logger.info("Kafka producer closed.")

@app.post("/events")
async def ingest_events(request: Request):
    """Endpoint for ingesting user events from the frontend tracker."""
    try:
        data = await request.json()
        events: List[Dict[str, Any]] = data.get("events", [])
        
        if not events:
            logger.warning("Received empty event list.")
            return {"message": "No events received"}

        producer = get_kafka_producer()
        if not producer:
            return {"message": "Kafka producer not available"}, 500

        for event in events:
            product_id = event.get("product_id")
            future = producer.send(USER_EVENTS_TOPIC, value=event, key=product_id)
            future.add_errback(lambda e: logger.error(f"Failed to send event to Kafka: {e}"))
        
        producer.flush()
        logger.info(f"Successfully ingested {len(events)} events.")
        return {"message": f"Ingested {len(events)} events"}
    except json.JSONDecodeError:
        logger.error("Invalid JSON received.")
        return {"message": "Invalid JSON"}, 400
    except Exception as e:
        logger.error(f"Error ingesting events: {e}", exc_info=True)
        return {"message": "Internal server error"}, 500

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected: {websocket.client}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"WebSocket disconnected: {websocket.client}")

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except RuntimeError as e:
                logger.error(f"Error sending to WebSocket {connection.client}: {e}. Removing connection.")
                self.active_connections.remove(connection)

manager = ConnectionManager()

async def consume_trending_products_kafka():
    """Consumes messages from the trending products Kafka topic and broadcasts them via WebSocket."""
    consumer = None
    while True:
        try:
            if consumer is None:
                consumer = KafkaConsumer(
                    TRENDING_PRODUCTS_TOPIC,
                    bootstrap_servers=[KAFKA_BROKER_URL],
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    group_id='trending-product-websocket-group',
                    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                )
                logger.info(f"Kafka consumer for trending products connected to {KAFKA_BROKER_URL}")

            for message in consumer:
                logger.info(f"Received trending product alert: {message.value}")
                
                # Update cache and alerts
                update_trending_products_cache(message.value)
                add_to_recent_alerts(message.value)
                
                # Send alerts if z-score is high enough
                z_score = message.value.get('z_score', 0)
                if z_score > 3.0:  # Configurable threshold
                    try:
                        alerting_service.send_alert(message.value)
                    except Exception as e:
                        logger.error(f"Failed to send alert: {e}")
                
                # Broadcast to WebSocket clients
                await manager.broadcast(json.dumps(message.value))
        except KafkaError as e:
            logger.error(f"Kafka consumer error: {e}. Reconnecting in 5 seconds...")
            consumer = None
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected error in Kafka consumer: {e}. Reconnecting in 5 seconds...")
            consumer = None
            await asyncio.sleep(5)

@app.websocket("/ws/trending")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)

@app.on_event("startup")
async def start_kafka_consumer_task():
    asyncio.create_task(consume_trending_products_kafka())

@app.get("/", response_class=HTMLResponse)
async def read_root():
    return """
    <!DOCTYPE html>
    <html>
        <head>
            <title>Herd Behavior Alerter Backend</title>
        </head>
        <body>
            <h1>Welcome to the Herd Behavior Alerter Backend!</h1>
            <p>This is the API for ingesting user events and serving real-time trending product alerts.</p>
            <p>Connect to the WebSocket endpoint at <code>ws://localhost:8000/ws/trending</code> to receive live alerts.</p>
            <p>Post events to <code>/events</code>.</p>
        </body>
    </html>
    """
