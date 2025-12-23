#!/usr/bin/env python3
"""
Alert Management Script for NYC TLC Data Platform

This script manages alert configurations, sends test alerts, and provides
utilities for alert management.
"""

import os
import json
import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import logging
from typing import Dict, Any, List


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('alert_management.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class AlertManager:
    """
    Manages alert configurations and sending for the NYC TLC Data Platform
    """
    
    def __init__(self):
        """
        Initialize the AlertManager with configuration from environment
        """
        self.smtp_host = os.getenv('SMTP_HOST', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.smtp_user = os.getenv('SMTP_USER', 'your_email@gmail.com')
        self.smtp_password = os.getenv('SMTP_PASSWORD', 'your_app_password')
        self.slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
        self.alert_email_recipients = os.getenv('ALERT_EMAIL_RECIPIENTS', 'data-team@example.com').split(',')
        
    def send_email_alert(self, subject: str, body: str, recipients: List[str] = None) -> bool:
        """
        Send an email alert
        
        Args:
            subject: Email subject
            body: Email body (HTML)
            recipients: List of email recipients (uses default if None)
            
        Returns:
            True if email was sent successfully, False otherwise
        """
        if recipients is None:
            recipients = self.alert_email_recipients
            
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.smtp_user
            msg['To'] = ', '.join(recipients)
            
            # Create HTML part
            html_part = MIMEText(body, 'html')
            msg.attach(html_part)
            
            # Connect to server and send email
            server = smtplib.SMTP(self.smtp_host, self.smtp_port)
            server.starttls()
            server.login(self.smtp_user, self.smtp_password)
            
            server.send_message(msg)
            server.quit()
            
            logger.info(f"Email alert sent successfully to {recipients}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")
            return False
    
    def send_slack_alert(self, message: str, channel: str = '#data-alerts') -> bool:
        """
        Send a Slack alert
        
        Args:
            message: Message to send
            channel: Slack channel to send to
            
        Returns:
            True if message was sent successfully, False otherwise
        """
        if not self.slack_webhook_url:
            logger.warning("SLACK_WEBHOOK_URL not configured, skipping Slack alert")
            return False
            
        try:
            payload = {
                'text': message,
                'channel': channel,
                'username': 'NYC TLC Data Platform Monitor',
                'icon_emoji': ':warning:'
            }
            
            response = requests.post(self.slack_webhook_url, json=payload)
            
            if response.status_code == 200:
                logger.info(f"Slack alert sent successfully to {channel}")
                return True
            else:
                logger.error(f"Failed to send Slack alert: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")
            return False
    
    def send_test_alert(self) -> bool:
        """
        Send a test alert to verify alerting system
        """
        logger.info("Sending test alert...")
        
        # Test email
        email_subject = "NYC TLC Data Platform - Test Alert"
        email_body = f"""
        <html>
        <body>
            <h2>NYC TLC Data Platform Test Alert</h2>
            <p>This is a test alert sent at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.</p>
            <p>If you received this alert, the email notification system is working correctly.</p>
            <hr>
            <p><em>NYC TLC Data Platform Monitoring System</em></p>
        </body>
        </html>
        """
        
        email_success = self.send_email_alert(email_subject, email_body)
        
        # Test Slack if configured
        slack_success = True
        if self.slack_webhook_url:
            slack_message = f"✅ NYC TLC Data Platform - Test Alert\nSent at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\nThis is a test message to verify the alerting system."
            slack_success = self.send_slack_alert(slack_message)
        
        success = email_success and slack_success
        if success:
            logger.info("Test alert sent successfully")
        else:
            logger.error("Test alert failed")
        
        return success
    
    def check_alert_status(self) -> Dict[str, Any]:
        """
        Check the status of alerting systems
        
        Returns:
            Dictionary with status information
        """
        status = {
            'timestamp': datetime.now().isoformat(),
            'email_configured': bool(self.smtp_user and self.smtp_password),
            'slack_configured': bool(self.slack_webhook_url),
            'alert_recipients': self.alert_email_recipients
        }
        
        # Test connectivity if possible
        if status['email_configured']:
            try:
                server = smtplib.SMTP(self.smtp_host, self.smtp_port)
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.quit()
                status['email_connectivity'] = True
            except:
                status['email_connectivity'] = False
        
        if status['slack_configured']:
            try:
                response = requests.post(
                    self.slack_webhook_url,
                    json={'text': 'Connection test', 'username': 'Test Bot'},
                    timeout=5
                )
                status['slack_connectivity'] = response.status_code == 200
            except:
                status['slack_connectivity'] = False
        
        return status
    
    def create_alert_from_pipeline_log(self, pipeline_name: str, run_id: str, 
                                     status: str, records_processed: int, 
                                     records_failed: int, error_message: str = None) -> bool:
        """
        Create an alert based on pipeline execution log
        
        Args:
            pipeline_name: Name of the pipeline
            run_id: Run ID
            status: Execution status
            records_processed: Number of records processed
            records_failed: Number of records that failed
            error_message: Error message if any
            
        Returns:
            True if alert was sent successfully, False otherwise
        """
        if status.lower() == 'failed':
            subject = f"🚨 NYC TLC Data Platform - Pipeline FAILED: {pipeline_name}"
            body = f"""
            <html>
            <body>
                <h2 style="color: red;">🚨 Pipeline FAILED</h2>
                <table border="1" cellpadding="5" cellspacing="0">
                    <tr><th>Pipeline</th><td>{pipeline_name}</td></tr>
                    <tr><th>Run ID</th><td>{run_id}</td></tr>
                    <tr><th>Status</th><td style="color: red;">{status}</td></tr>
                    <tr><th>Records Processed</th><td>{records_processed}</td></tr>
                    <tr><th>Records Failed</th><td style="color: red;">{records_failed}</td></tr>
                    <tr><th>Error Message</th><td>{error_message or 'N/A'}</td></tr>
                    <tr><th>Time</th><td>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</td></tr>
                </table>
                <p>Please investigate and take corrective action.</p>
                <hr>
                <p><em>NYC TLC Data Platform Monitoring System</em></p>
            </body>
            </html>
            """
            
            # Send email alert
            email_success = self.send_email_alert(subject, body)
            
            # Send Slack alert
            slack_success = True
            if self.slack_webhook_url:
                slack_message = f"🚨 Pipeline FAILED: {pipeline_name}\nRun ID: {run_id}\nRecords Failed: {records_failed}\nError: {error_message or 'N/A'}"
                slack_success = self.send_slack_alert(slack_message, '#pipeline-alerts')
            
            return email_success and slack_success
        
        elif records_failed > 0 and records_processed > 0:
            # Partial failure
            subject = f"⚠️ NYC TLC Data Platform - Pipeline PARTIAL FAILURE: {pipeline_name}"
            body = f"""
            <html>
            <body>
                <h2 style="color: orange;">⚠️ Pipeline PARTIAL FAILURE</h2>
                <table border="1" cellpadding="5" cellspacing="0">
                    <tr><th>Pipeline</th><td>{pipeline_name}</td></tr>
                    <tr><th>Run ID</th><td>{run_id}</td></tr>
                    <tr><th>Status</th><td style="color: orange;">{status}</td></tr>
                    <tr><th>Records Processed</th><td>{records_processed}</td></tr>
                    <tr><th>Records Failed</th><td style="color: red;">{records_failed}</td></tr>
                    <tr><th>Error Message</th><td>{error_message or 'N/A'}</td></tr>
                    <tr><th>Time</th><td>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</td></tr>
                </table>
                <p>Please review the failed records and take appropriate action.</p>
                <hr>
                <p><em>NYC TLC Data Platform Monitoring System</em></p>
            </body>
            </html>
            """
            
            # Send email alert
            email_success = self.send_email_alert(subject, body)
            
            # Send Slack alert
            slack_success = True
            if self.slack_webhook_url:
                slack_message = f"⚠️ Pipeline PARTIAL FAILURE: {pipeline_name}\nRun ID: {run_id}\nRecords Failed: {records_failed}"
                slack_success = self.send_slack_alert(slack_message, '#pipeline-alerts')
            
            return email_success and slack_success
        
        return True  # No alert needed for successful runs


def main():
    """
    Main function to run alert management utilities
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Alert Management for NYC TLC Data Platform')
    parser.add_argument('action', choices=['test', 'status', 'create-pipeline-alert'], 
                       help='Action to perform')
    parser.add_argument('--pipeline-name', help='Pipeline name (for create-pipeline-alert)')
    parser.add_argument('--run-id', help='Run ID (for create-pipeline-alert)')
    parser.add_argument('--status', help='Status (for create-pipeline-alert)')
    parser.add_argument('--records-processed', type=int, default=0, 
                       help='Records processed (for create-pipeline-alert)')
    parser.add_argument('--records-failed', type=int, default=0, 
                       help='Records failed (for create-pipeline-alert)')
    parser.add_argument('--error-message', help='Error message (for create-pipeline-alert)')
    
    args = parser.parse_args()
    
    alert_manager = AlertManager()
    
    if args.action == 'test':
        success = alert_manager.send_test_alert()
        exit(0 if success else 1)
    
    elif args.action == 'status':
        status = alert_manager.check_alert_status()
        print(json.dumps(status, indent=2))
    
    elif args.action == 'create-pipeline-alert':
        if not all([args.pipeline_name, args.run_id, args.status]):
            print("Error: pipeline-name, run-id, and status are required for create-pipeline-alert")
            exit(1)
        
        success = alert_manager.create_alert_from_pipeline_log(
            args.pipeline_name, args.run_id, args.status,
            args.records_processed, args.records_failed, args.error_message
        )
        exit(0 if success else 1)


if __name__ == "__main__":
    main()