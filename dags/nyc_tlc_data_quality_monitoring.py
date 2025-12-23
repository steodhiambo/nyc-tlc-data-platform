"""
NYC TLC Data Quality Monitoring DAG

This DAG monitors data quality metrics and sends alerts when thresholds are exceeded.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.email.operators.email import EmailOperator
from sqlalchemy import create_engine
import logging
import os
from typing import Dict, List


# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['data-team@example.com']
}

# Initialize the DAG
dag = DAG(
    'nyc_tlc_data_quality_monitoring',
    default_args=default_args,
    description='Monitor data quality metrics and send alerts',
    schedule_interval='@hourly',  # Run every hour
    catchup=False,
    max_active_runs=1,
    tags=['nyc-tlc', 'monitoring', 'data-quality', 'alerts']
)


def check_data_quality_metrics(**context):
    """
    Check data quality metrics and identify any issues
    """
    logging.info("Starting data quality checks")
    
    # Get PostgreSQL connection details
    user = os.getenv('POSTGRES_USER', 'nyc_tlc_user')
    password = os.getenv('POSTGRES_PASSWORD', 'nyc_tlc_password')
    host = os.getenv('POSTGRES_HOST', 'datawarehouse')
    port = os.getenv('POSTGRES_PORT', '5432')
    database = os.getenv('POSTGRES_DB', 'nyc_tlc_dw')
    
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    
    try:
        # Create SQLAlchemy engine
        engine = create_engine(connection_string)
        
        # Define quality thresholds
        thresholds = {
            'null_pickup_datetime_pct': 1.0,  # Alert if more than 1% are null
            'null_dropoff_datetime_pct': 1.0,  # Alert if more than 1% are null
            'negative_fare_pct': 0.5,  # Alert if more than 0.5% are negative
            'avg_trip_distance': (0.1, 50.0)  # Alert if avg trip distance is < 0.1 or > 50 miles
        }
        
        # Get latest quality metrics
        query = """
        SELECT table_name, metric_name, metric_value, date, created_at
        FROM taxi_data.data_quality_metrics
        WHERE date = CURRENT_DATE
        ORDER BY created_at DESC
        LIMIT 100
        """
        
        with engine.connect() as conn:
            result = conn.execute(query)
            metrics = result.fetchall()
        
        issues = []
        
        for row in metrics:
            table_name, metric_name, metric_value, date, created_at = row
            
            if metric_name in thresholds:
                threshold = thresholds[metric_name]
                
                if metric_name.endswith('_pct'):
                    # Percentage threshold
                    if metric_value > threshold:
                        issues.append({
                            'table': table_name,
                            'metric': metric_name,
                            'value': metric_value,
                            'threshold': threshold,
                            'message': f'{metric_name} is {metric_value}% which exceeds threshold of {threshold}%'
                        })
                elif isinstance(threshold, tuple) and len(threshold) == 2:
                    # Range threshold
                    min_val, max_val = threshold
                    if metric_value < min_val or metric_value > max_val:
                        issues.append({
                            'table': table_name,
                            'metric': metric_name,
                            'value': metric_value,
                            'threshold': f'{min_val}-{max_val}',
                            'message': f'{metric_name} is {metric_value} which is outside threshold range of {min_val}-{max_val}'
                        })
        
        logging.info(f"Found {len(issues)} data quality issues")
        
        # Store issues in XCom for downstream tasks
        context['task_instance'].xcom_push(key='quality_issues', value=issues)
        
        return len(issues)
        
    except Exception as e:
        logging.error(f"Error checking data quality metrics: {e}")
        raise


def check_pipeline_status(**context):
    """
    Check pipeline execution status for failures or delays
    """
    logging.info("Checking pipeline execution status")
    
    # Get PostgreSQL connection details
    user = os.getenv('POSTGRES_USER', 'nyc_tlc_user')
    password = os.getenv('POSTGRES_PASSWORD', 'nyc_tlc_password')
    host = os.getenv('POSTGRES_HOST', 'datawarehouse')
    port = os.getenv('POSTGRES_PORT', '5432')
    database = os.getenv('POSTGRES_DB', 'nyc_tlc_dw')
    
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    
    try:
        # Create SQLAlchemy engine
        engine = create_engine(connection_string)
        
        # Check for failed runs in the last 24 hours
        failed_runs_query = """
        SELECT pipeline_name, run_id, status, start_time, end_time, error_message
        FROM taxi_data.pipeline_logs
        WHERE created_at >= NOW() - INTERVAL '24 hours'
        AND status = 'FAILED'
        """
        
        with engine.connect() as conn:
            failed_runs_result = conn.execute(failed_runs_query)
            failed_runs = failed_runs_result.fetchall()
        
        # Check for runs that are older than expected (potential delays)
        delay_check_query = """
        SELECT pipeline_name, run_id, status, start_time, end_time
        FROM taxi_data.pipeline_logs
        WHERE created_at >= NOW() - INTERVAL '2 hours'
        AND status = 'RUNNING'
        """
        
        with engine.connect() as conn:
            delay_check_result = conn.execute(delay_check_query)
            delayed_runs = delay_check_result.fetchall()
        
        issues = []
        
        for run in failed_runs:
            pipeline_name, run_id, status, start_time, end_time, error_message = run
            issues.append({
                'type': 'failure',
                'pipeline': pipeline_name,
                'run_id': run_id,
                'message': f'Pipeline {pipeline_name} failed at run {run_id}. Error: {error_message}'
            })
        
        for run in delayed_runs:
            pipeline_name, run_id, status, start_time, end_time = run
            issues.append({
                'type': 'delay',
                'pipeline': pipeline_name,
                'run_id': run_id,
                'message': f'Pipeline {pipeline_name} run {run_id} is taking longer than expected'
            })
        
        logging.info(f"Found {len(issues)} pipeline status issues")
        
        # Store issues in XCom for downstream tasks
        context['task_instance'].xcom_push(key='pipeline_issues', value=issues)
        
        return len(issues)
        
    except Exception as e:
        logging.error(f"Error checking pipeline status: {e}")
        raise


def send_slack_alerts(**context):
    """
    Send Slack alerts for data quality and pipeline issues
    """
    logging.info("Preparing Slack alerts")
    
    # Get issues from XCom
    quality_issues = context['task_instance'].xcom_pull(key='quality_issues', task_ids='check_data_quality')
    pipeline_issues = context['task_instance'].xcom_pull(key='pipeline_issues', task_ids='check_pipeline_status')
    
    all_issues = []
    all_issues.extend(quality_issues or [])
    all_issues.extend(pipeline_issues or [])
    
    if not all_issues:
        logging.info("No issues to alert on")
        return
    
    # Prepare Slack message
    message = f"🚨 *Data Quality/Pipeline Alert*\n\n"
    message += f"Found {len(all_issues)} issues at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}:\n\n"
    
    for i, issue in enumerate(all_issues, 1):
        if 'table' in issue:  # Quality issue
            message += f"{i}. Quality Issue: {issue['message']}\n"
        elif 'type' in issue:  # Pipeline issue
            message += f"{i}. {issue['type'].title()} Issue: {issue['message']}\n"
    
    message += f"\nPlease investigate and take appropriate action."
    
    # Get Slack webhook URL from environment
    slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
    
    if slack_webhook_url:
        # Use Airflow's SlackWebhookOperator to send the message
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
        
        hook = SlackWebhookHook(
            http_conn_id=None,
            webhook_token=slack_webhook_url,
            message=message,
            channel='#data-alerts',
            username='Data Quality Monitor'
        )
        
        hook.execute()
        logging.info("Slack alerts sent successfully")
    else:
        logging.warning("SLACK_WEBHOOK_URL not set, skipping Slack alerts")


def send_email_alerts(**context):
    """
    Send email alerts for data quality and pipeline issues
    """
    logging.info("Preparing email alerts")
    
    # Get issues from XCom
    quality_issues = context['task_instance'].xcom_pull(key='quality_issues', task_ids='check_data_quality')
    pipeline_issues = context['task_instance'].xcom_pull(key='pipeline_issues', task_ids='check_pipeline_status')
    
    all_issues = []
    all_issues.extend(quality_issues or [])
    all_issues.extend(pipeline_issues or [])
    
    if not all_issues:
        logging.info("No issues to email about")
        return
    
    # Prepare email content
    subject = f"🚨 NYC TLC Data Platform - Quality & Pipeline Alert - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    
    body = f"""
    <html>
    <body>
        <h2>NYC TLC Data Platform - Quality & Pipeline Alert</h2>
        <p>Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p>Found {len(all_issues)} issues:</p>
        
        <ul>
    """
    
    for issue in all_issues:
        if 'table' in issue:  # Quality issue
            body += f"<li><strong>Quality Issue:</strong> {issue['message']}</li>\n"
        elif 'type' in issue:  # Pipeline issue
            body += f"<li><strong>{issue['type'].title()} Issue:</strong> {issue['message']}</li>\n"
    
    body += """
        </ul>
        
        <p>Please investigate and take appropriate action.</p>
        
        <p>Best regards,<br>
        NYC TLC Data Platform Monitoring System</p>
    </body>
    </html>
    """
    
    # Get email recipients from environment or use default
    email_recipients = os.getenv('ALERT_EMAIL_RECIPIENTS', 'data-team@example.com').split(',')
    
    # Use Airflow's EmailOperator to send the email
    from airflow.utils.email import send_email
    
    send_email(
        to=email_recipients,
        subject=subject,
        html_content=body
    )
    
    logging.info(f"Email alerts sent to: {email_recipients}")


# Define tasks in the DAG
with dag:
    # Start task
    start_task = DummyOperator(
        task_id='start_monitoring',
        dag=dag
    )
    
    # Check data quality metrics
    check_quality_task = PythonOperator(
        task_id='check_data_quality',
        python_callable=check_data_quality_metrics,
        dag=dag
    )
    
    # Check pipeline status
    check_pipeline_task = PythonOperator(
        task_id='check_pipeline_status',
        python_callable=check_pipeline_status,
        dag=dag
    )
    
    # Send Slack alerts if needed
    send_slack_task = PythonOperator(
        task_id='send_slack_alerts',
        python_callable=send_slack_alerts,
        dag=dag
    )
    
    # Send email alerts if needed
    send_email_task = PythonOperator(
        task_id='send_email_alerts',
        python_callable=send_email_alerts,
        dag=dag
    )
    
    # End task
    end_task = DummyOperator(
        task_id='end_monitoring',
        dag=dag
    )
    
    # Set task dependencies
    start_task >> [check_quality_task, check_pipeline_task]
    check_quality_task >> send_slack_task
    check_pipeline_task >> send_slack_task
    send_slack_task >> send_email_task >> end_task


if __name__ == "__main__":
    # For testing purposes
    dag.test()