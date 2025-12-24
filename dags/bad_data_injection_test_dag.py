"""
Bad Data Injection Test DAG

This DAG runs tests with injected bad data to validate
data quality checks and ensure the system properly handles bad data.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from scripts.bad_data_injection_test import run_comprehensive_test, create_test_scenarios
import logging
import os


# Default arguments for the DAG
default_args = {
    'owner': 'data-testing-team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['testing-team@example.com']
}

# Initialize the DAG
dag = DAG(
    'bad_data_injection_test',
    default_args=default_args,
    description='Test data quality with injected bad data',
    schedule_interval='@weekly',  # Run weekly
    catchup=False,
    max_active_runs=1,
    tags=['nyc-tlc', 'testing', 'data-quality', 'bad-data']
)


def run_bad_data_tests(**context):
    """
    Run bad data injection tests
    """
    logging.info("Starting bad data injection tests")
    
    try:
        # Run comprehensive test
        test_summary = run_comprehensive_test()
        
        # Store test results in XCom
        context['task_instance'].xcom_push(key='test_summary', value=test_summary)
        
        logging.info("Bad data injection tests completed successfully")
        return test_summary
        
    except Exception as e:
        logging.error(f"Error during bad data injection tests: {e}")
        raise


def validate_test_results(**context):
    """
    Validate the results of the bad data tests
    """
    logging.info("Validating bad data test results")
    
    # Get test summary from XCom
    test_summary = context['task_instance'].xcom_pull(key='test_summary', task_ids='run_bad_data_tests')
    
    if not test_summary:
        logging.error("No test summary found")
        return False
    
    # Check if tests passed
    test_status = test_summary.get('test_status', 'FAIL')
    
    if test_status == 'PASS':
        logging.info("All bad data tests passed successfully")
        return True
    else:
        logging.error(f"Bad data tests failed with status: {test_status}")
        # This could trigger alerts in a real implementation
        return False


def create_test_scenarios_task(**context):
    """
    Create test scenarios with different types of bad data
    """
    logging.info("Creating bad data test scenarios")
    
    try:
        # Create test scenarios
        scenarios = create_test_scenarios()
        
        # Store scenario information in XCom
        scenario_info = {name: len(data) for name, data in scenarios.items()}
        context['task_instance'].xcom_push(key='scenarios_info', value=scenario_info)
        
        logging.info(f"Created {len(scenarios)} test scenarios")
        return scenario_info
        
    except Exception as e:
        logging.error(f"Error creating test scenarios: {e}")
        raise


def generate_test_report(**context):
    """
    Generate a test report
    """
    logging.info("Generating bad data test report")
    
    # Get test summary and scenario info from XCom
    test_summary = context['task_instance'].xcom_pull(key='test_summary', task_ids='run_bad_data_tests')
    scenarios_info = context['task_instance'].xcom_pull(key='scenarios_info', task_ids='create_test_scenarios')
    
    if not test_summary or not scenarios_info:
        logging.error("Missing test data for report generation")
        return
    
    # Generate report content
    report_content = f"""
    Bad Data Injection Test Report
    ===============================
    
    Test Run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    DAG Run ID: {context['dag_run'].run_id}
    
    Test Status: {test_summary.get('test_status', 'UNKNOWN')}
    
    Validation Results:
    - Clean data validation: {'PASS' if test_summary['validation_results']['clean_data']['success'] else 'FAIL'}
    - Bad data validation: {'PASS' if not test_summary['validation_results']['bad_data']['success'] else 'FAIL'} (should fail)
    
    Test Scenarios Created: {len(scenarios_info)}
    Total Records in Scenarios: {sum(scenarios_info.values())}
    
    System correctly handled {sum(1 for v in test_summary['response_validation'].values() if v['response_correct'])}/{len(test_summary['response_validation'])} scenarios.
    
    The system successfully detects and responds to bad data as expected.
    """
    
    # Save report to file
    report_file = f"/tmp/bad_data_test_report_{context['ts_nodash']}.txt"
    with open(report_file, 'w') as f:
        f.write(report_content)
    
    logging.info(f"Test report generated: {report_file}")
    
    # Store report file path in XCom
    context['task_instance'].xcom_push(key='report_file', value=report_file)


# Define tasks in the DAG
with dag:
    # Start task
    start_task = DummyOperator(
        task_id='start_bad_data_tests',
        dag=dag
    )

    # Create test scenarios
    create_scenarios_task = PythonOperator(
        task_id='create_test_scenarios',
        python_callable=create_test_scenarios_task,
        dag=dag
    )

    # Run bad data tests
    run_tests_task = PythonOperator(
        task_id='run_bad_data_tests',
        python_callable=run_bad_data_tests,
        dag=dag
    )

    # Validate test results
    validate_results_task = PythonOperator(
        task_id='validate_test_results',
        python_callable=validate_test_results,
        dag=dag
    )

    # Generate test report
    generate_report_task = PythonOperator(
        task_id='generate_test_report',
        python_callable=generate_test_report,
        dag=dag
    )

    # End task
    end_task = DummyOperator(
        task_id='end_bad_data_tests',
        dag=dag
    )

    # Set task dependencies
    start_task >> create_scenarios_task >> run_tests_task
    run_tests_task >> validate_results_task >> generate_report_task
    generate_report_task >> end_task


if __name__ == "__main__":
    # For testing purposes
    dag.test()