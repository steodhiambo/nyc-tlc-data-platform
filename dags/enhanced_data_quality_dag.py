"""
Enhanced Data Quality DAG

This DAG implements the enhanced data quality framework with:
- Advanced statistical validation
- Data profiling
- Data lineage tracking
- Caching layer
- ClickHouse integration
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.email.operators.email import EmailOperator
from enhanced_data_quality import EnhancedDataQualityFramework, run_comprehensive_data_quality_check
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
    'enhanced_data_quality_framework',
    default_args=default_args,
    description='Run enhanced data quality checks with profiling, lineage, and analytics',
    schedule_interval='@daily',  # Run daily
    catchup=False,
    max_active_runs=1,
    tags=['nyc-tlc', 'data-quality', 'enhanced-validation', 'profiling', 'lineage', 'analytics']
)


def run_enhanced_data_quality_checks(**context):
    """
    Run enhanced data quality checks including statistical validation, profiling, and lineage tracking
    """
    logging.info("Starting enhanced data quality checks")
    
    try:
        # Initialize the enhanced data quality framework
        framework = EnhancedDataQualityFramework()
        
        # List of tables to validate
        tables_to_validate = ['yellow_tripdata', 'green_tripdata']
        
        validation_results = {}
        
        for table_name in tables_to_validate:
            logging.info(f"Running enhanced validation for {table_name}")
            
            # Run comprehensive data quality check
            result = run_comprehensive_data_quality_check(table_name)
            validation_results[table_name] = result
            
            # Cache validation results
            framework.cache_dataset(f"validation_results_{table_name}", result)
        
        # Store results in XCom for downstream tasks
        context['task_instance'].xcom_push(
            key='enhanced_validation_results', 
            value=validation_results
        )
        
        logging.info("Enhanced data quality checks completed successfully")
        return validation_results
        
    except Exception as e:
        logging.error(f"Error during enhanced data quality checks: {e}")
        raise


def run_data_profiling(**context):
    """
    Run advanced data profiling to detect schema changes and anomalies
    """
    logging.info("Starting data profiling")
    
    try:
        framework = EnhancedDataQualityFramework()
        
        # Get validation results from XCom
        validation_results = context['task_instance'].xcom_pull(
            key='enhanced_validation_results', 
            task_ids='run_enhanced_data_quality_checks'
        )
        
        profiling_summary = {}
        
        for table_name, results in validation_results.items():
            if 'profiling_results' in results and results['profiling_results']:
                profiling_info = results['profiling_results']
                
                # Check for anomalies
                anomalies = profiling_info.get('anomalies', [])
                schema_changes = profiling_info.get('schema_changes', {})
                
                profiling_summary[table_name] = {
                    'anomaly_count': len(anomalies),
                    'schema_change_count': sum(len(changes) for changes in schema_changes.values()),
                    'has_anomalies': len(anomalies) > 0,
                    'has_schema_changes': sum(len(changes) for changes in schema_changes.values()) > 0
                }
                
                # Log anomalies if any
                if anomalies:
                    logging.warning(f"Found {len(anomalies)} anomalies in {table_name}:")
                    for anomaly in anomalies:
                        logging.warning(f"  - {anomaly['message']} (Severity: {anomaly['severity']})")
                
                # Log schema changes if any
                if any(len(changes) > 0 for changes in schema_changes.values()):
                    logging.info(f"Found schema changes in {table_name}:")
                    for change_type, changes in schema_changes.items():
                        if changes:
                            logging.info(f"  {change_type}: {changes}")
        
        # Store profiling summary in XCom
        context['task_instance'].xcom_push(key='profiling_summary', value=profiling_summary)
        
        logging.info("Data profiling completed")
        return profiling_summary
        
    except Exception as e:
        logging.error(f"Error during data profiling: {e}")
        raise


def run_analytical_queries(**context):
    """
    Run analytical queries using ClickHouse if available
    """
    logging.info("Starting analytical queries")
    
    try:
        framework = EnhancedDataQualityFramework()
        
        if not framework.clickhouse_available:
            logging.warning("ClickHouse not available, skipping analytical queries")
            analytical_results = {"clickhouse_unavailable": True}
        else:
            # Run some analytical queries
            queries = {
                "monthly_trips": """
                    SELECT 
                        toMonth(pickup_datetime) as month,
                        count(*) as trip_count
                    FROM nyc_tlc_trips
                    WHERE toYear(pickup_datetime) = 2023
                    GROUP BY month
                    ORDER BY month
                """,
                "top_payment_types": """
                    SELECT 
                        payment_type,
                        count(*) as count,
                        avg(total_amount) as avg_amount
                    FROM nyc_tlc_trips
                    GROUP BY payment_type
                    ORDER BY count DESC
                    LIMIT 10
                """
            }
            
            analytical_results = {}
            for query_name, query in queries.items():
                result_df = framework.run_analytical_query(query, use_cache=True)
                analytical_results[query_name] = result_df.to_dict('records') if not result_df.empty else []
        
        # Store analytical results in XCom
        context['task_instance'].xcom_push(key='analytical_results', value=analytical_results)
        
        logging.info("Analytical queries completed")
        return analytical_results
        
    except Exception as e:
        logging.error(f"Error during analytical queries: {e}")
        raise


def check_data_lineage(**context):
    """
    Check data lineage and generate lineage reports
    """
    logging.info("Checking data lineage")
    
    try:
        framework = EnhancedDataQualityFramework()
        
        # Get validation results from XCom
        validation_results = context['task_instance'].xcom_pull(
            key='enhanced_validation_results', 
            task_ids='run_enhanced_data_quality_checks'
        )
        
        lineage_analysis = {}
        
        for table_name in validation_results.keys():
            # Get lineage info
            lineage_info = framework.get_lineage_info(table_name)
            
            # Perform impact analysis
            impact_analysis = framework.get_impact_analysis(table_name)
            
            # Perform source analysis
            source_analysis = framework.get_source_analysis(table_name)
            
            lineage_analysis[table_name] = {
                'lineage_events_count': len(lineage_info.get('events', [])),
                'upstream_datasets': lineage_info.get('upstream_datasets', []),
                'downstream_datasets': lineage_info.get('downstream_datasets', []),
                'impact_analysis': impact_analysis,
                'source_analysis': source_analysis
            }
        
        # Store lineage analysis in XCom
        context['task_instance'].xcom_push(key='lineage_analysis', value=lineage_analysis)
        
        logging.info("Data lineage check completed")
        return lineage_analysis
        
    except Exception as e:
        logging.error(f"Error during data lineage check: {e}")
        raise


def generate_quality_report(**context):
    """
    Generate a comprehensive quality report
    """
    logging.info("Generating quality report")
    
    try:
        # Get all results from XCom
        validation_results = context['task_instance'].xcom_pull(
            key='enhanced_validation_results', 
            task_ids='run_enhanced_data_quality_checks'
        )
        profiling_summary = context['task_instance'].xcom_pull(
            key='profiling_summary', 
            task_ids='run_data_profiling'
        )
        analytical_results = context['task_instance'].xcom_pull(
            key='analytical_results', 
            task_ids='run_analytical_queries'
        )
        lineage_analysis = context['task_instance'].xcom_pull(
            key='lineage_analysis', 
            task_ids='check_data_lineage'
        )
        
        # Generate summary report
        report = {
            'summary': {
                'tables_validated': list(validation_results.keys()),
                'validation_success': all(
                    result['validation_results'].success if result['validation_results'] else False
                    for result in validation_results.values()
                ),
                'anomaly_count': sum(
                    summary.get('anomaly_count', 0) 
                    for summary in profiling_summary.values()
                ),
                'total_lineage_events': sum(
                    analysis.get('lineage_events_count', 0) 
                    for analysis in lineage_analysis.values()
                )
            },
            'validation_results': validation_results,
            'profiling_summary': profiling_summary,
            'analytical_results': analytical_results,
            'lineage_analysis': lineage_analysis,
            'generated_at': datetime.now().isoformat()
        }
        
        # Store report in XCom
        context['task_instance'].xcom_push(key='quality_report', value=report)
        
        # Save report to file
        import json
        report_file = f"/tmp/nyc_tlc_quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logging.info(f"Quality report generated and saved to {report_file}")
        return report_file
        
    except Exception as e:
        logging.error(f"Error generating quality report: {e}")
        raise


def send_quality_alerts(**context):
    """
    Send alerts based on quality check results
    """
    logging.info("Preparing quality alerts")
    
    try:
        # Get report from XCom
        report_file = context['task_instance'].xcom_pull(
            key='quality_report',
            task_ids='generate_quality_report'
        )
        
        # Load report
        import json
        with open(report_file, 'r') as f:
            report = json.load(f)
        
        # Check for issues
        issues_found = []
        
        # Check validation results
        validation_results = report.get('validation_results', {})
        for table_name, result in validation_results.items():
            if not result.get('validation_results', {}).get('validation_results', {}).get('success', True):
                issues_found.append(f"Validation failed for {table_name}")
        
        # Check profiling summary
        profiling_summary = report.get('profiling_summary', {})
        for table_name, summary in profiling_summary.items():
            if summary.get('has_anomalies', False):
                anomaly_count = summary.get('anomaly_count', 0)
                issues_found.append(f"{anomaly_count} anomalies found in {table_name}")
            
            if summary.get('has_schema_changes', False):
                issues_found.append(f"Schema changes detected in {table_name}")
        
        if not issues_found:
            logging.info("No issues found, no alerts to send")
            return
        
        # Prepare Slack message
        message = f"🚨 *NYC TLC Enhanced Data Quality Alert*\n\n"
        message += f"Issues found at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}:\n\n"
        
        for i, issue in enumerate(issues_found, 1):
            message += f"{i}. {issue}\n\n"
        
        message += f"Please investigate and take appropriate action.\n\n"
        message += f"Report file: {report_file}"
        
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
                username='Enhanced Data Quality Monitor'
            )
            
            hook.execute()
            logging.info("Slack alerts sent successfully")
        else:
            logging.warning("SLACK_WEBHOOK_URL not set, skipping Slack alerts")
        
        # Prepare email content
        subject = f"🚨 NYC TLC Enhanced Data Quality Alert - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        body = f"""
        <html>
        <body>
            <h2>NYC TLC Enhanced Data Quality Alert</h2>
            <p>Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p>Issues found:</p>

            <ul>
        """
        
        for issue in issues_found:
            body += f"<li>{issue}</li>\n"
        
        body += f"""
            </ul>
            <p>Report file: {report_file}</p>

            <p>Please investigate and take appropriate action.</p>

            <p>Best regards,<br>
            NYC TLC Enhanced Data Quality System</p>
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
        
    except Exception as e:
        logging.error(f"Error sending quality alerts: {e}")
        raise


# Define tasks in the DAG
with dag:
    # Start task
    start_task = DummyOperator(
        task_id='start_enhanced_quality',
        dag=dag
    )
    
    # Run enhanced data quality checks
    run_quality_checks_task = PythonOperator(
        task_id='run_enhanced_data_quality_checks',
        python_callable=run_enhanced_data_quality_checks,
        dag=dag
    )
    
    # Run data profiling
    run_profiling_task = PythonOperator(
        task_id='run_data_profiling',
        python_callable=run_data_profiling,
        dag=dag
    )
    
    # Run analytical queries
    run_analytics_task = PythonOperator(
        task_id='run_analytical_queries',
        python_callable=run_analytical_queries,
        dag=dag
    )
    
    # Check data lineage
    check_lineage_task = PythonOperator(
        task_id='check_data_lineage',
        python_callable=check_data_lineage,
        dag=dag
    )
    
    # Generate quality report
    generate_report_task = PythonOperator(
        task_id='generate_quality_report',
        python_callable=generate_quality_report,
        dag=dag
    )
    
    # Send alerts
    send_alerts_task = PythonOperator(
        task_id='send_quality_alerts',
        python_callable=send_quality_alerts,
        dag=dag
    )
    
    # End task
    end_task = DummyOperator(
        task_id='end_enhanced_quality',
        dag=dag
    )
    
    # Set task dependencies
    start_task >> run_quality_checks_task
    run_quality_checks_task >> [run_profiling_task, run_analytics_task, check_lineage_task]
    run_profiling_task >> generate_report_task
    run_analytics_task >> generate_report_task
    check_lineage_task >> generate_report_task
    generate_report_task >> send_alerts_task >> end_task


if __name__ == "__main__":
    # For testing purposes
    dag.test()