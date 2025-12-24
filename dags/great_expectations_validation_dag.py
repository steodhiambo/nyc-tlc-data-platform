"""
Great Expectations Validation DAG

This DAG runs data quality checks using Great Expectations
and sends alerts when validation thresholds are exceeded.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.email.operators.email import EmailOperator
from scripts.great_expectations_validation import run_data_quality_checks, create_quality_alerts, save_validation_results
import logging
import os


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
    'great_expectations_validation',
    default_args=default_args,
    description='Run Great Expectations data quality validation',
    schedule_interval='@daily',  # Run daily
    catchup=False,
    max_active_runs=1,
    tags=['nyc-tlc', 'data-quality', 'great-expectations', 'validation']
)


def run_ge_validation(**context):
    """
    Run Great Expectations validation on taxi data
    """
    logging.info("Starting Great Expectations validation")
    
    try:
        # Run data quality checks
        validation_results = run_data_quality_checks()
        
        # Store results in XCom for downstream tasks
        context['task_instance'].xcom_push(key='validation_results', value=validation_results)
        
        # Save validation results
        run_id = context['ts'].replace('-', '').replace(':', '').replace('T', '_').split('.')[0]
        save_validation_results(validation_results, run_id)
        
        logging.info("Great Expectations validation completed successfully")
        return validation_results
        
    except Exception as e:
        logging.error(f"Error during Great Expectations validation: {e}")
        raise


def check_validation_alerts(**context):
    """
    Check validation results and create alerts if needed
    """
    logging.info("Checking validation results for alerts")
    
    # Get validation results from XCom
    validation_results = context['task_instance'].xcom_pull(key='validation_results', task_ids='run_ge_validation')
    
    if not validation_results:
        logging.warning("No validation results found")
        return []
    
    # Create alerts based on validation results
    alerts = create_quality_alerts(validation_results)
    
    # Store alerts in XCom for downstream tasks
    context['task_instance'].xcom_push(key='alerts', value=alerts)
    
    logging.info(f"Found {len(alerts)} alerts to send")
    return alerts


def send_validation_alerts_slack(**context):
    """
    Send validation alerts via Slack
    """
    logging.info("Preparing Slack alerts for validation results")
    
    # Get alerts from XCom
    alerts = context['task_instance'].xcom_pull(key='alerts', task_ids='check_validation_alerts')
    
    if not alerts:
        logging.info("No alerts to send via Slack")
        return
    
    # Prepare Slack message
    message = f"🚨 *Great Expectations Validation Alert*\n\n"
    message += f"Found {len(alerts)} validation issues at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}:\n\n"
    
    for i, alert in enumerate(alerts, 1):
        message += f"{i}. {alert['table']}: {alert['message']}\n"
        message += f"   Severity: {alert['severity']}\n\n"
    
    message += f"Please investigate and take appropriate action."

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
            username='GE Validation Monitor'
        )

        hook.execute()
        logging.info("Slack alerts sent successfully")
    else:
        logging.warning("SLACK_WEBHOOK_URL not set, skipping Slack alerts")


def send_validation_alerts_email(**context):
    """
    Send validation alerts via email
    """
    logging.info("Preparing email alerts for validation results")
    
    # Get alerts from XCom
    alerts = context['task_instance'].xcom_pull(key='alerts', task_ids='check_validation_alerts')
    
    if not alerts:
        logging.info("No alerts to send via email")
        return
    
    # Prepare email content
    subject = f"🚨 NYC TLC Data - GE Validation Alert - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    body = f"""
    <html>
    <body>
        <h2>NYC TLC Data - Great Expectations Validation Alert</h2>
        <p>Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p>Found {len(alerts)} validation issues:</p>

        <ul>
    """

    for alert in alerts:
        severity_class = "high" if alert['severity'] == "high" else "medium"
        body += f"<li><strong>{alert['table']}</strong>: {alert['message']} (Severity: {alert['severity']})</li>\n"

    body += """
        </ul>

        <p>Please investigate and take appropriate action.</p>

        <p>Best regards,<br>
        NYC TLC Data Platform GE Validation System</p>
    </body>
    </html>
    """

    # Get email recipients from environment or use default
    email_recipients = os.getenv('ALERT_EMAIL_RECIPIENTS', 'data-team@example.com').split(',')

    # Send email
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
        task_id='start_validation',
        dag=dag
    )

    # Run Great Expectations validation
    run_validation_task = PythonOperator(
        task_id='run_ge_validation',
        python_callable=run_ge_validation,
        dag=dag
    )

    # Check validation results for alerts
    check_alerts_task = PythonOperator(
        task_id='check_validation_alerts',
        python_callable=check_validation_alerts,
        dag=dag
    )

    # Send Slack alerts if needed
    send_slack_alerts_task = PythonOperator(
        task_id='send_validation_alerts_slack',
        python_callable=send_validation_alerts_slack,
        dag=dag
    )

    # Send email alerts if needed
    send_email_alerts_task = PythonOperator(
        task_id='send_validation_alerts_email',
        python_callable=send_validation_alerts_email,
        dag=dag
    )

    # End task
    end_task = DummyOperator(
        task_id='end_validation',
        dag=dag
    )

    # Set task dependencies
    start_task >> run_validation_task >> check_alerts_task
    check_alerts_task >> [send_slack_alerts_task, send_email_alerts_task]
    [send_slack_alerts_task, send_email_alerts_task] >> end_task


if __name__ == "__main__":
    # For testing purposes
    dag.test()