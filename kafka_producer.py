"""
Kafka Producer for Herd Behavior Events
Handles event ingestion from the JavaScript tracker
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Any
from kafka import KafkaProducer
from kafka.errors import KafkaError
import asyncio
import threading

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HerdEventProducer:
    def __init__(self, 
                 bootstrap_servers: List[str] = ['localhost:9094'],
                 topic: str = 'user_events',
                 batch_size: int = 100,
                 linger_ms: int = 100):
        """
        Initialize Kafka producer for herd behavior events
        
        Args:
            bootstrap_servers: List of Kafka broker addresses
            topic: Kafka topic name for user events
            batch_size: Number of events to batch before sending
            linger_ms: Time to wait for batching
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.batch_size = batch_size
        self.linger_ms = linger_ms
        
        self.producer = None
        self.is_connected = False
        self._connect()
    
    def _connect(self):
        """Establish connection to Kafka"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                batch_size=self.batch_size,
                linger_ms=self.linger_ms,
                retries=3,
                acks='all',  # Wait for all replicas to acknowledge
                compression_type='gzip'
            )
            self.is_connected = True
            logger.info(f"Connected to Kafka brokers: {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            self.is_connected = False
    
    def send_event(self, event: Dict[str, Any]) -> bool:
        """
        Send a single event to Kafka
        
        Args:
            event: Event data dictionary
            
        Returns:
            bool: True if sent successfully, False otherwise
        """
        if not self.is_connected:
            logger.warning("Kafka producer not connected")
            return False
        
        try:
            # Enrich event with metadata
            enriched_event = self._enrich_event(event)
            
            # Use product_id as partition key for better distribution
            partition_key = enriched_event.get('product_id', enriched_event.get('user_id'))
            
            # Send to Kafka
            future = self.producer.send(
                self.topic,
                value=enriched_event,
                key=partition_key
            )
            
            # Add callback for monitoring
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)
            
            return True
            
        except Exception as e:
            logger.error(f"Error sending event: {e}")
            return False
    
    def send_events_batch(self, events: List[Dict[str, Any]]) -> int:
        """
        Send multiple events to Kafka
        
        Args:
            events: List of event dictionaries
            
        Returns:
            int: Number of events sent successfully
        """
        if not self.is_connected:
            logger.warning("Kafka producer not connected")
            return 0
        
        sent_count = 0
        for event in events:
            if self.send_event(event):
                sent_count += 1
        
        # Flush to ensure all events are sent
        self.producer.flush()
        
        logger.info(f"Sent {sent_count}/{len(events)} events to Kafka")
        return sent_count
    
    def _enrich_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich event with additional metadata
        
        Args:
            event: Original event data
            
        Returns:
            Dict: Enriched event data
        """
        enriched = event.copy()
        
        # Add server-side timestamp
        enriched['server_timestamp'] = datetime.utcnow().isoformat()
        
        # Add event ID for deduplication
        if 'event_id' not in enriched:
            enriched['event_id'] = f"{enriched.get('user_id', 'unknown')}_{enriched.get('timestamp', datetime.utcnow().isoformat())}"
        
        # Normalize event type
        if 'event_type' in enriched:
            enriched['event_type'] = enriched['event_type'].lower()
        
        # Add event version for schema evolution
        enriched['event_version'] = '1.0'
        
        return enriched
    
    def _on_send_success(self, record_metadata):
        """Callback for successful sends"""
        logger.debug(f"Event sent to topic {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
    
    def _on_send_error(self, exception):
        """Callback for send errors"""
        logger.error(f"Failed to send event: {exception}")
    
    def close(self):
        """Close the producer connection"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")

class EventBuffer:
    """
    Buffer for collecting events before sending to Kafka
    Useful for high-throughput scenarios
    """
    
    def __init__(self, producer: HerdEventProducer, max_size: int = 1000, flush_interval: int = 5):
        self.producer = producer
        self.max_size = max_size
        self.flush_interval = flush_interval
        self.buffer = []
        self.lock = threading.Lock()
        self.running = True
        
        # Start background flush thread
        self.flush_thread = threading.Thread(target=self._periodic_flush, daemon=True)
        self.flush_thread.start()
    
    def add_event(self, event: Dict[str, Any]):
        """Add event to buffer"""
        with self.lock:
            self.buffer.append(event)
            
            # Flush if buffer is full
            if len(self.buffer) >= self.max_size:
                self._flush()
    
    def _flush(self):
        """Flush buffer to Kafka"""
        if not self.buffer:
            return
        
        events_to_send = self.buffer.copy()
        self.buffer.clear()
        
        # Send events in background
        threading.Thread(
            target=self.producer.send_events_batch,
            args=(events_to_send,),
            daemon=True
        ).start()
    
    def _periodic_flush(self):
        """Periodically flush buffer"""
        import time
        
        while self.running:
            time.sleep(self.flush_interval)
            with self.lock:
                if self.buffer:
                    self._flush()
    
    def stop(self):
        """Stop the buffer and flush remaining events"""
        self.running = False
        with self.lock:
            self._flush()

# Example usage and testing
if __name__ == "__main__":
    # Initialize producer
    producer = HerdEventProducer()
    
    # Test events
    test_events = [
        {
            "event_type": "view_product",
            "user_id": "user_123",
            "session_id": "session_456",
            "product_id": "prod_001",
            "product_name": "Wireless Headphones",
            "product_price": "99.99",
            "timestamp": datetime.utcnow().isoformat(),
            "url": "https://example.com/product/wireless-headphones"
        },
        {
            "event_type": "add_to_cart",
            "user_id": "user_123",
            "session_id": "session_456",
            "product_id": "prod_001",
            "product_name": "Wireless Headphones",
            "product_price": "99.99",
            "quantity": 1,
            "timestamp": datetime.utcnow().isoformat(),
            "url": "https://example.com/product/wireless-headphones"
        }
    ]
    
    # Send test events
    if producer.is_connected:
        producer.send_events_batch(test_events)
        print("Test events sent successfully")
    else:
        print("Failed to connect to Kafka")
    
    # Close producer
    producer.close()
