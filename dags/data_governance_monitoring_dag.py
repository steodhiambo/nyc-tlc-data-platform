"""
Data Governance Monitoring DAG

This DAG monitors data governance aspects including:
- Asset registration
- Lineage tracking
- Access logging
- Compliance checks
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.email.operators.email import EmailOperator
from scripts.data_governance import DataGovernanceManager, register_nyc_tlc_data_assets
import logging
import os


# Default arguments for the DAG
default_args = {
    'owner': 'data-governance-team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['governance-team@example.com']
}

# Initialize the DAG
dag = DAG(
    'data_governance_monitoring',
    default_args=default_args,
    description='Monitor data governance aspects',
    schedule_interval='@daily',  # Run daily
    catchup=False,
    max_active_runs=1,
    tags=['nyc-tlc', 'governance', 'lineage', 'compliance']
)


def register_data_assets(**context):
    """
    Register data assets in the governance system
    """
    logging.info("Registering data assets in governance system")
    
    try:
        # Register NYC TLC data assets
        assets = register_nyc_tlc_data_assets()
        
        # Store number of assets registered in XCom
        context['task_instance'].xcom_push(key='assets_registered', value=len(assets))
        
        logging.info(f"Successfully registered {len(assets)} data assets")
        return len(assets)
        
    except Exception as e:
        logging.error(f"Error registering data assets: {e}")
        raise


def check_data_lineage(**context):
    """
    Check data lineage and identify any issues
    """
    logging.info("Checking data lineage for issues")
    
    try:
        governance_manager = DataGovernanceManager()
        
        # List of critical assets to check lineage for
        critical_assets = [
            'fact_trips',
            'staging_yellow_tripdata',
            'staging_green_tripdata'
        ]
        
        issues = []
        
        for asset_name in critical_assets:
            lineage = governance_manager.get_lineage(asset_name)
            
            # Check if upstream dependencies exist
            if not lineage['upstream']:
                issues.append({
                    'asset': asset_name,
                    'type': 'missing_upstream',
                    'message': f'No upstream dependencies found for {asset_name}'
                })
            
            # Check if downstream dependencies exist
            if not lineage['downstream']:
                issues.append({
                    'asset': asset_name,
                    'type': 'missing_downstream',
                    'message': f'No downstream dependencies found for {asset_name} (potential orphaned asset)'
                })
        
        # Store issues in XCom for downstream tasks
        context['task_instance'].xcom_push(key='lineage_issues', value=issues)
        
        logging.info(f"Found {len(issues)} lineage issues")
        return len(issues)
        
    except Exception as e:
        logging.error(f"Error checking data lineage: {e}")
        raise


def check_compliance(**context):
    """
    Check compliance with data governance policies
    """
    logging.info("Checking compliance with data governance policies")
    
    try:
        issues = []
        
        # Check if all critical tables have owners
        critical_tables = [
            'yellow_tripdata',
            'green_tripdata',
            'fact_trips',
            'dim_location'
        ]
        
        governance_manager = DataGovernanceManager()
        
        for table in critical_tables:
            asset = governance_manager.get_data_asset(table)
            if not asset or not asset.owner:
                issues.append({
                    'table': table,
                    'type': 'missing_owner',
                    'message': f'Missing owner for critical table {table}'
                })
        
        # Check if all tables have descriptions
        for table in critical_tables:
            asset = governance_manager.get_data_asset(table)
            if not asset or not asset.description:
                issues.append({
                    'table': table,
                    'type': 'missing_description',
                    'message': f'Missing description for critical table {table}'
                })
        
        # Store issues in XCom for downstream tasks
        context['task_instance'].xcom_push(key='compliance_issues', value=issues)
        
        logging.info(f"Found {len(issues)} compliance issues")
        return len(issues)
        
    except Exception as e:
        logging.error(f"Error checking compliance: {e}")
        raise


def send_governance_alerts(**context):
    """
    Send alerts for governance issues
    """
    logging.info("Preparing governance alerts")
    
    # Get issues from XCom
    lineage_issues = context['task_instance'].xcom_pull(key='lineage_issues', task_ids='check_data_lineage')
    compliance_issues = context['task_instance'].xcom_pull(key='compliance_issues', task_ids='check_compliance')
    
    all_issues = []
    all_issues.extend(lineage_issues or [])
    all_issues.extend(compliance_issues or [])
    
    if not all_issues:
        logging.info("No governance issues to alert on")
        return
    
    # Prepare Slack message
    message = f"📋 *Data Governance Alert*\n\n"
    message += f"Found {len(all_issues)} governance issues at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}:\n\n"
    
    for i, issue in enumerate(all_issues, 1):
        message += f"{i}. {issue['type'].replace('_', ' ').title()}: {issue['message']}\n\n"
    
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
            channel='#governance-alerts',
            username='Data Governance Monitor'
        )

        hook.execute()
        logging.info("Slack governance alerts sent successfully")
    else:
        logging.warning("SLACK_WEBHOOK_URL not set, skipping Slack alerts")


def send_governance_email_alerts(**context):
    """
    Send email alerts for governance issues
    """
    logging.info("Preparing governance email alerts")

    # Get issues from XCom
    lineage_issues = context['task_instance'].xcom_pull(key='lineage_issues', task_ids='check_data_lineage')
    compliance_issues = context['task_instance'].xcom_pull(key='compliance_issues', task_ids='check_compliance')

    all_issues = []
    all_issues.extend(lineage_issues or [])
    all_issues.extend(compliance_issues or [])

    if not all_issues:
        logging.info("No governance issues to email about")
        return

    # Prepare email content
    subject = f"📋 NYC TLC Data Platform - Governance Alert - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    body = f"""
    <html>
    <body>
        <h2>NYC TLC Data Platform - Data Governance Alert</h2>
        <p>Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p>Found {len(all_issues)} governance issues:</p>

        <ul>
    """

    for issue in all_issues:
        body += f"<li><strong>{issue['type'].replace('_', ' ').title()}:</strong> {issue['message']}</li>\n"

    body += """
        </ul>

        <p>Please investigate and take appropriate action.</p>

        <p>Best regards,<br>
        NYC TLC Data Platform Governance System</p>
    </body>
    </html>
    """

    # Get email recipients from environment or use default
    email_recipients = os.getenv('GOVERNANCE_ALERT_EMAIL_RECIPIENTS', 'governance-team@example.com').split(',')

    # Send email
    from airflow.utils.email import send_email

    send_email(
        to=email_recipients,
        subject=subject,
        html_content=body
    )

    logging.info(f"Email governance alerts sent to: {email_recipients}")


# Define tasks in the DAG
with dag:
    # Start task
    start_task = DummyOperator(
        task_id='start_governance_monitoring',
        dag=dag
    )

    # Register data assets
    register_assets_task = PythonOperator(
        task_id='register_data_assets',
        python_callable=register_data_assets,
        dag=dag
    )

    # Check data lineage
    check_lineage_task = PythonOperator(
        task_id='check_data_lineage',
        python_callable=check_data_lineage,
        dag=dag
    )

    # Check compliance
    check_compliance_task = PythonOperator(
        task_id='check_compliance',
        python_callable=check_compliance,
        dag=dag
    )

    # Send Slack alerts if needed
    send_slack_alerts_task = PythonOperator(
        task_id='send_governance_alerts',
        python_callable=send_governance_alerts,
        dag=dag
    )

    # Send email alerts if needed
    send_email_alerts_task = PythonOperator(
        task_id='send_governance_email_alerts',
        python_callable=send_governance_email_alerts,
        dag=dag
    )

    # End task
    end_task = DummyOperator(
        task_id='end_governance_monitoring',
        dag=dag
    )

    # Set task dependencies
    start_task >> register_assets_task
    register_assets_task >> [check_lineage_task, check_compliance_task]
    check_lineage_task >> send_slack_alerts_task
    check_compliance_task >> send_slack_alerts_task
    send_slack_alerts_task >> send_email_alerts_task >> end_task


if __name__ == "__main__":
    # For testing purposes
    dag.test()