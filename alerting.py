import smtplib
import json
import logging
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Any, Optional
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class AlertingService:
    """Service for sending alerts via email and Slack."""
    
    def __init__(self):
        # Email configuration
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.email_user = os.getenv('EMAIL_USER', '')
        self.email_password = os.getenv('EMAIL_PASSWORD', '')
        self.from_email = os.getenv('FROM_EMAIL', self.email_user)
        
        # Slack configuration
        self.slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL', '')
        
        # Alert recipients
        self.email_recipients = os.getenv('ALERT_EMAIL_RECIPIENTS', '').split(',')
        self.email_recipients = [email.strip() for email in self.email_recipients if email.strip()]
        
    def send_email_alert(self, product_data: Dict[str, Any]) -> bool:
        """Send email alert for trending product."""
        if not self.email_user or not self.email_password or not self.email_recipients:
            logger.warning("Email configuration incomplete. Skipping email alert.")
            return False
            
        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = self.from_email
            msg['To'] = ', '.join(self.email_recipients)
            msg['Subject'] = f"🚨 Trending Product Alert: Product {product_data.get('product_id', 'Unknown')}"
            
            # Create email body
            body = self._create_email_body(product_data)
            msg.attach(MIMEText(body, 'html'))
            
            # Send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.email_user, self.email_password)
                server.send_message(msg)
                
            logger.info(f"Email alert sent for product {product_data.get('product_id')}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")
            return False
    
    def send_slack_alert(self, product_data: Dict[str, Any]) -> bool:
        """Send Slack alert for trending product."""
        if not self.slack_webhook_url:
            logger.warning("Slack webhook URL not configured. Skipping Slack alert.")
            return False
            
        try:
            # Create Slack message
            message = self._create_slack_message(product_data)
            
            # Send to Slack
            response = requests.post(
                self.slack_webhook_url,
                json=message,
                timeout=10
            )
            response.raise_for_status()
            
            logger.info(f"Slack alert sent for product {product_data.get('product_id')}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")
            return False
    
    def send_alert(self, product_data: Dict[str, Any]) -> Dict[str, bool]:
        """Send alerts via all configured channels."""
        results = {
            'email': False,
            'slack': False
        }
        
        # Send email alert
        if self.email_recipients:
            results['email'] = self.send_email_alert(product_data)
        
        # Send Slack alert
        if self.slack_webhook_url:
            results['slack'] = self.send_slack_alert(product_data)
        
        return results
    
    def _create_email_body(self, product_data: Dict[str, Any]) -> str:
        """Create HTML email body for trending product alert."""
        z_score = product_data.get('z_score', 0)
        current_activity = product_data.get('current_activity', 0)
        baseline_mean = product_data.get('baseline_mean', 0)
        product_id = product_data.get('product_id', 'Unknown')
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Determine severity
        if z_score > 5:
            severity = "🔥 VIRAL"
            color = "#dc2626"
        elif z_score > 4:
            severity = "🚨 HIGH"
            color = "#ea580c"
        elif z_score > 3:
            severity = "⚠️ MEDIUM"
            color = "#ca8a04"
        else:
            severity = "📈 LOW"
            color = "#2563eb"
        
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }}
                .container {{ max-width: 600px; margin: 0 auto; background-color: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
                .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; text-align: center; }}
                .content {{ padding: 30px; }}
                .alert-box {{ background-color: {color}; color: white; padding: 15px; border-radius: 6px; margin: 20px 0; text-align: center; font-weight: bold; }}
                .metrics {{ background-color: #f8f9fa; padding: 20px; border-radius: 6px; margin: 20px 0; }}
                .metric {{ display: inline-block; margin: 10px 20px; text-align: center; }}
                .metric-value {{ font-size: 24px; font-weight: bold; color: #333; }}
                .metric-label {{ font-size: 12px; color: #666; text-transform: uppercase; }}
                .footer {{ background-color: #f8f9fa; padding: 20px; text-align: center; color: #666; font-size: 12px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🎯 Herd Behavior Alert</h1>
                    <p>Real-time E-commerce Trend Detection</p>
                </div>
                
                <div class="content">
                    <div class="alert-box">
                        {severity} TRENDING PRODUCT DETECTED
                    </div>
                    
                    <h2>Product {product_id} is showing anomalous activity!</h2>
                    
                    <p>Our anomaly detection system has identified unusual user behavior patterns for this product. This could indicate:</p>
                    <ul>
                        <li>🔥 Viral social media mention</li>
                        <li>📺 Media coverage or influencer promotion</li>
                        <li>💰 Price drop or special promotion</li>
                        <li>📦 Limited stock creating urgency</li>
                        <li>🎯 Organic customer interest surge</li>
                    </ul>
                    
                    <div class="metrics">
                        <div class="metric">
                            <div class="metric-value">{z_score:.2f}</div>
                            <div class="metric-label">Z-Score</div>
                        </div>
                        <div class="metric">
                            <div class="metric-value">{current_activity}</div>
                            <div class="metric-label">Current Activity</div>
                        </div>
                        <div class="metric">
                            <div class="metric-value">{baseline_mean:.1f}</div>
                            <div class="metric-label">Baseline Average</div>
                        </div>
                    </div>
                    
                    <h3>Recommended Actions:</h3>
                    <ul>
                        <li>📊 Check inventory levels immediately</li>
                        <li>🎯 Consider promoting this product</li>
                        <li>📱 Monitor social media mentions</li>
                        <li>💼 Alert marketing and sales teams</li>
                        <li>📈 Prepare for increased demand</li>
                    </ul>
                </div>
                
                <div class="footer">
                    <p>Alert generated at {timestamp}</p>
                    <p>Herd Behavior Alerter - Real-time E-commerce Analytics</p>
                </div>
            </div>
        </body>
        </html>
        """
    
    def _create_slack_message(self, product_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create Slack message for trending product alert."""
        z_score = product_data.get('z_score', 0)
        current_activity = product_data.get('current_activity', 0)
        baseline_mean = product_data.get('baseline_mean', 0)
        product_id = product_data.get('product_id', 'Unknown')
        
        # Determine severity and emoji
        if z_score > 5:
            severity = "🔥 VIRAL"
            color = "danger"
        elif z_score > 4:
            severity = "🚨 HIGH"
            color = "warning"
        elif z_score > 3:
            severity = "⚠️ MEDIUM"
            color = "warning"
        else:
            severity = "📈 LOW"
            color = "good"
        
        return {
            "text": f"🎯 Trending Product Alert: Product {product_id}",
            "attachments": [
                {
                    "color": color,
                    "title": f"{severity} - Product {product_id} is trending!",
                    "text": "Anomalous user activity detected. This could indicate viral content, media coverage, or organic interest surge.",
                    "fields": [
                        {
                            "title": "Z-Score",
                            "value": f"{z_score:.2f}",
                            "short": True
                        },
                        {
                            "title": "Current Activity",
                            "value": str(current_activity),
                            "short": True
                        },
                        {
                            "title": "Baseline Average",
                            "value": f"{baseline_mean:.1f}",
                            "short": True
                        },
                        {
                            "title": "Timestamp",
                            "value": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            "short": True
                        }
                    ],
                    "footer": "Herd Behavior Alerter",
                    "footer_icon": "https://platform.slack-edge.com/img/default_application_icon.png"
                }
            ]
        }

# Global alerting service instance
alerting_service = AlertingService()
