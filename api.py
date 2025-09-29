from fastapi import APIRouter, HTTPException, BackgroundTasks
from typing import List, Dict, Any, Optional
import json
import logging
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from herd_behavior_alerter.backend.alerting import alerting_service

logger = logging.getLogger(__name__)

router = APIRouter()

# In-memory storage for trending products (in production, use Redis or database)
trending_products_cache = {}
recent_alerts = []

KAFKA_BROKER_URL = "localhost:9094"
TRENDING_PRODUCTS_TOPIC = "trending_products"

@router.get("/api/trending-products")
async def get_trending_products(limit: int = 10) -> List[Dict[str, Any]]:
    """Get current trending products."""
    try:
        # Return cached trending products sorted by z-score
        products = list(trending_products_cache.values())
        products.sort(key=lambda x: x.get('z_score', 0), reverse=True)
        return products[:limit]
    except Exception as e:
        logger.error(f"Error fetching trending products: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch trending products")

@router.get("/api/trending-products/{product_id}")
async def get_product_details(product_id: str) -> Dict[str, Any]:
    """Get details for a specific trending product."""
    try:
        if product_id not in trending_products_cache:
            raise HTTPException(status_code=404, detail="Product not found in trending list")
        
        return trending_products_cache[product_id]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching product details: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch product details")

@router.get("/api/alerts")
async def get_recent_alerts(limit: int = 20) -> List[Dict[str, Any]]:
    """Get recent alerts."""
    try:
        return recent_alerts[:limit]
    except Exception as e:
        logger.error(f"Error fetching alerts: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch alerts")

@router.post("/api/alerts/test")
async def test_alert(
    background_tasks: BackgroundTasks,
    product_id: str = "TEST_001",
    z_score: float = 4.5
) -> Dict[str, Any]:
    """Test the alerting system with a mock alert."""
    try:
        # Create test alert data
        test_alert = {
            "product_id": product_id,
            "z_score": z_score,
            "current_activity": 150,
            "baseline_mean": 25.5,
            "baseline_std": 8.2,
            "message": f"Product {product_id} is showing anomalous activity (Z-Score: {z_score:.2f})",
            "timestamp": datetime.now().isoformat(),
            "alert_type": "test"
        }
        
        # Send alerts in background
        background_tasks.add_task(send_test_alerts, test_alert)
        
        # Add to recent alerts
        recent_alerts.insert(0, test_alert)
        if len(recent_alerts) > 100:
            recent_alerts.pop()
        
        return {
            "message": "Test alert initiated",
            "alert_data": test_alert
        }
    except Exception as e:
        logger.error(f"Error sending test alert: {e}")
        raise HTTPException(status_code=500, detail="Failed to send test alert")

@router.get("/api/stats")
async def get_system_stats() -> Dict[str, Any]:
    """Get system statistics."""
    try:
        now = datetime.now()
        
        # Calculate stats from cached data
        total_trending = len(trending_products_cache)
        high_priority = sum(1 for p in trending_products_cache.values() if p.get('z_score', 0) > 4)
        
        # Recent alerts in last hour
        one_hour_ago = now - timedelta(hours=1)
        recent_alert_count = sum(
            1 for alert in recent_alerts 
            if datetime.fromisoformat(alert.get('timestamp', '1970-01-01')) > one_hour_ago
        )
        
        return {
            "total_trending_products": total_trending,
            "high_priority_alerts": high_priority,
            "alerts_last_hour": recent_alert_count,
            "system_status": "operational",
            "last_updated": now.isoformat()
        }
    except Exception as e:
        logger.error(f"Error fetching system stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch system stats")

@router.delete("/api/trending-products/{product_id}")
async def dismiss_trending_product(product_id: str) -> Dict[str, str]:
    """Dismiss a trending product from the list."""
    try:
        if product_id not in trending_products_cache:
            raise HTTPException(status_code=404, detail="Product not found in trending list")
        
        del trending_products_cache[product_id]
        
        return {"message": f"Product {product_id} dismissed from trending list"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error dismissing product: {e}")
        raise HTTPException(status_code=500, detail="Failed to dismiss product")

@router.post("/api/alerts/configure")
async def configure_alerts(config: Dict[str, Any]) -> Dict[str, str]:
    """Configure alert settings (placeholder for future implementation)."""
    try:
        # In a real implementation, this would update alert configuration
        # For now, just return success
        logger.info(f"Alert configuration updated: {config}")
        
        return {"message": "Alert configuration updated successfully"}
    except Exception as e:
        logger.error(f"Error configuring alerts: {e}")
        raise HTTPException(status_code=500, detail="Failed to configure alerts")

async def send_test_alerts(alert_data: Dict[str, Any]):
    """Send test alerts via all configured channels."""
    try:
        results = alerting_service.send_alert(alert_data)
        logger.info(f"Test alert results: {results}")
    except Exception as e:
        logger.error(f"Error sending test alerts: {e}")

def update_trending_products_cache(product_data: Dict[str, Any]):
    """Update the trending products cache with new data."""
    try:
        product_id = product_data.get('product_id')
        if product_id:
            trending_products_cache[product_id] = {
                **product_data,
                'last_updated': datetime.now().isoformat()
            }
            
            # Keep only the most recent 50 trending products
            if len(trending_products_cache) > 50:
                # Remove the oldest entry
                oldest_key = min(
                    trending_products_cache.keys(),
                    key=lambda k: trending_products_cache[k].get('last_updated', '1970-01-01')
                )
                del trending_products_cache[oldest_key]
                
    except Exception as e:
        logger.error(f"Error updating trending products cache: {e}")

def add_to_recent_alerts(alert_data: Dict[str, Any]):
    """Add alert to recent alerts list."""
    try:
        recent_alerts.insert(0, {
            **alert_data,
            'timestamp': datetime.now().isoformat()
        })
        
        # Keep only the most recent 100 alerts
        if len(recent_alerts) > 100:
            recent_alerts.pop()
            
    except Exception as e:
        logger.error(f"Error adding to recent alerts: {e}")
