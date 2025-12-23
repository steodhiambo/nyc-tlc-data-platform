#!/usr/bin/env python3
"""
Grafana Dashboard Configuration Script

This script creates Grafana dashboards for monitoring the NYC TLC Data Platform.
"""

import requests
import json
import logging
import os
from typing import Dict, Any


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('grafana_setup.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class GrafanaSetup:
    """
    Sets up Grafana dashboards for the NYC TLC Data Platform
    """
    
    def __init__(self, grafana_url: str = None, api_key: str = None):
        """
        Initialize Grafana client
        
        Args:
            grafana_url: URL of the Grafana instance
            api_key: API key for authentication
        """
        self.grafana_url = grafana_url or os.getenv('GRAFANA_URL', 'http://localhost:3000')
        self.api_key = api_key or os.getenv('GRAFANA_API_KEY', 'admin')
        
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_key}'
        }
    
    def create_dashboard(self, dashboard_json: Dict[str, Any], folder_id: int = 0) -> bool:
        """
        Create a dashboard in Grafana
        
        Args:
            dashboard_json: Dashboard configuration in JSON format
            folder_id: Folder ID to store the dashboard (0 for General)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            payload = {
                'dashboard': dashboard_json,
                'folderId': folder_id,
                'overwrite': True
            }
            
            response = requests.post(
                f"{self.grafana_url}/api/dashboards/db",
                headers=self.headers,
                data=json.dumps(payload)
            )
            
            if response.status_code in [200, 201]:
                logger.info(f"Dashboard '{dashboard_json['title']}' created successfully")
                return True
            else:
                logger.error(f"Failed to create dashboard: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            logger.error(f"Error creating dashboard: {e}")
            return False
    
    def create_nyc_tlc_overview_dashboard(self) -> Dict[str, Any]:
        """
        Create the main NYC TLC Data Platform overview dashboard
        """
        dashboard = {
            "dashboard": {
                "id": None,
                "title": "NYC TLC Data Platform - Overview",
                "tags": ["nyc-tlc", "etl", "overview"],
                "style": "dark",
                "timezone": "browser",
                "refresh": "5m",
                "schemaVersion": 16,
                "version": 0,
                "time": {
                    "from": "now-24h",
                    "to": "now"
                },
                "timepicker": {
                    "refresh_intervals": ["5s", "10s", "30s", "1m", "5m", "15m", "30m", "1h", "2h", "1d"]
                },
                "templating": {
                    "list": []
                },
                "annotations": {
                    "list": []
                },
                "links": [],
                "panels": [
                    {
                        "id": 1,
                        "title": "Pipeline Status",
                        "type": "stat",
                        "gridPos": {"h": 6, "w": 6, "x": 0, "y": 0},
                        "targets": [
                            {
                                "expr": "sum(airflow_task_instance_count{state=\"success\"})",
                                "legendFormat": "Successful Tasks"
                            },
                            {
                                "expr": "sum(airflow_task_instance_count{state=\"failed\"})",
                                "legendFormat": "Failed Tasks"
                            }
                        ],
                        "fieldConfig": {
                            "defaults": {
                                "custom": {},
                                "mappings": [],
                                "thresholds": {
                                    "mode": "absolute",
                                    "steps": [
                                        {"color": "green", "value": None},
                                        {"color": "red", "value": 80}
                                    ]
                                }
                            }
                        }
                    },
                    {
                        "id": 2,
                        "title": "Data Processing Volume",
                        "type": "graph",
                        "gridPos": {"h": 6, "w": 12, "x": 6, "y": 0},
                        "targets": [
                            {
                                "expr": "rate(taxi_data_pipeline_logs_records_processed[5m])",
                                "legendFormat": "Records Processed Rate"
                            }
                        ],
                        "yAxes": [
                            {"label": "Records/second", "show": True},
                            {"show": True}
                        ]
                    },
                    {
                        "id": 3,
                        "title": "System Resources",
                        "type": "row",
                        "gridPos": {"h": 1, "w": 24, "x": 0, "y": 6},
                        "collapsed": False
                    },
                    {
                        "id": 4,
                        "title": "CPU Usage",
                        "type": "graph",
                        "gridPos": {"h": 7, "w": 8, "x": 0, "y": 7},
                        "targets": [
                            {
                                "expr": "100 - (avg by(instance) (rate(node_cpu_seconds_total{mode=\"idle\"}[5m])) * 100)",
                                "legendFormat": "{{instance}}"
                            }
                        ],
                        "yAxes": [
                            {"label": "CPU %", "show": True, "min": 0, "max": 100},
                            {"show": True}
                        ]
                    },
                    {
                        "id": 5,
                        "title": "Memory Usage",
                        "type": "graph",
                        "gridPos": {"h": 7, "w": 8, "x": 8, "y": 7},
                        "targets": [
                            {
                                "expr": "(node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes) / node_memory_MemTotal_bytes * 100",
                                "legendFormat": "{{instance}}"
                            }
                        ],
                        "yAxes": [
                            {"label": "Memory %", "show": True, "min": 0, "max": 100},
                            {"show": True}
                        ]
                    },
                    {
                        "id": 6,
                        "title": "Disk Usage",
                        "type": "graph",
                        "gridPos": {"h": 7, "w": 8, "x": 16, "y": 7},
                        "targets": [
                            {
                                "expr": "(node_filesystem_size_bytes - node_filesystem_free_bytes) / node_filesystem_size_bytes * 100",
                                "legendFormat": "{{instance}}:{{mountpoint}}"
                            }
                        ],
                        "yAxes": [
                            {"label": "Disk %", "show": True, "min": 0, "max": 100},
                            {"show": True}
                        ]
                    },
                    {
                        "id": 7,
                        "title": "Data Quality Metrics",
                        "type": "row",
                        "gridPos": {"h": 1, "w": 24, "x": 0, "y": 14},
                        "collapsed": False
                    },
                    {
                        "id": 8,
                        "title": "Null Pickup Datetime Percentage",
                        "type": "graph",
                        "gridPos": {"h": 7, "w": 12, "x": 0, "y": 15},
                        "targets": [
                            {
                                "expr": "taxi_data_quality_metrics{metric_name=\"null_pickup_datetime_pct\"}",
                                "legendFormat": "{{table_name}}"
                            }
                        ],
                        "yAxes": [
                            {"label": "%", "show": True},
                            {"show": True}
                        ]
                    },
                    {
                        "id": 9,
                        "title": "Average Trip Distance",
                        "type": "graph",
                        "gridPos": {"h": 7, "w": 12, "x": 12, "y": 15},
                        "targets": [
                            {
                                "expr": "taxi_data_quality_metrics{metric_name=\"avg_trip_distance\"}",
                                "legendFormat": "{{table_name}}"
                            }
                        ],
                        "yAxes": [
                            {"label": "Miles", "show": True},
                            {"show": True}
                        ]
                    },
                    {
                        "id": 10,
                        "title": "PostgreSQL Metrics",
                        "type": "row",
                        "gridPos": {"h": 1, "w": 24, "x": 0, "y": 22},
                        "collapsed": False
                    },
                    {
                        "id": 11,
                        "title": "Database Connections",
                        "type": "graph",
                        "gridPos": {"h": 7, "w": 12, "x": 0, "y": 23},
                        "targets": [
                            {
                                "expr": "pg_stat_activity_count",
                                "legendFormat": "Active Connections"
                            }
                        ],
                        "yAxes": [
                            {"label": "Connections", "show": True},
                            {"show": True}
                        ]
                    },
                    {
                        "id": 12,
                        "title": "Query Duration (95th percentile)",
                        "type": "graph",
                        "gridPos": {"h": 7, "w": 12, "x": 12, "y": 23},
                        "targets": [
                            {
                                "expr": "histogram_quantile(0.95, rate(pg_query_duration_seconds_bucket[5m]))",
                                "legendFormat": "95th Percentile Query Duration"
                            }
                        ],
                        "yAxes": [
                            {"label": "Seconds", "show": True},
                            {"show": True}
                        ]
                    }
                ],
                "schemaVersion": 16,
                "version": 1
            }
        }
        
        return dashboard
    
    def create_airflow_monitoring_dashboard(self) -> Dict[str, Any]:
        """
        Create the Airflow monitoring dashboard
        """
        dashboard = {
            "dashboard": {
                "id": None,
                "title": "NYC TLC Data Platform - Airflow Monitoring",
                "tags": ["nyc-tlc", "airflow", "etl"],
                "style": "dark",
                "timezone": "browser",
                "refresh": "5m",
                "schemaVersion": 16,
                "version": 0,
                "time": {
                    "from": "now-24h",
                    "to": "now"
                },
                "timepicker": {
                    "refresh_intervals": ["5s", "10s", "30s", "1m", "5m", "15m", "30m", "1h", "2h", "1d"]
                },
                "templating": {
                    "list": [
                        {
                            "name": "pipeline_name",
                            "type": "query",
                            "query": "label_values(airflow_dag_run_status_count, dag_id)",
                            "current": {"value": {}, "text": "All"},
                            "includeAll": True,
                            "multi": True
                        }
                    ]
                },
                "annotations": {
                    "list": []
                },
                "links": [],
                "panels": [
                    {
                        "id": 1,
                        "title": "DAG Run Status",
                        "type": "graph",
                        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
                        "targets": [
                            {
                                "expr": "airflow_dag_run_status_count{status=\"success\"}",
                                "legendFormat": "Successful Runs"
                            },
                            {
                                "expr": "airflow_dag_run_status_count{status=\"failed\"}",
                                "legendFormat": "Failed Runs"
                            },
                            {
                                "expr": "airflow_dag_run_status_count{status=\"running\"}",
                                "legendFormat": "Running Runs"
                            }
                        ]
                    },
                    {
                        "id": 2,
                        "title": "Task Instance Status",
                        "type": "graph",
                        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0},
                        "targets": [
                            {
                                "expr": "airflow_task_instance_count",
                                "legendFormat": "{{state}}"
                            }
                        ]
                    },
                    {
                        "id": 3,
                        "title": "Worker Status",
                        "type": "stat",
                        "gridPos": {"h": 5, "w": 6, "x": 0, "y": 8},
                        "targets": [
                            {
                                "expr": "airflow_worker_alive_gauge",
                                "legendFormat": "Alive Workers"
                            }
                        ],
                        "fieldConfig": {
                            "defaults": {
                                "custom": {},
                                "mappings": [],
                                "thresholds": {
                                    "mode": "absolute",
                                    "steps": [
                                        {"color": "red", "value": None},
                                        {"color": "yellow", "value": 1},
                                        {"color": "green", "value": 2}
                                    ]
                                }
                            }
                        }
                    },
                    {
                        "id": 4,
                        "title": "Scheduler Health",
                        "type": "singlestat",
                        "gridPos": {"h": 5, "w": 6, "x": 6, "y": 8},
                        "targets": [
                            {
                                "expr": "airflow_scheduler_heartbeat",
                                "legendFormat": "Scheduler Heartbeat"
                            }
                        ],
                        "fieldConfig": {
                            "defaults": {
                                "custom": {},
                                "mappings": [],
                                "thresholds": {
                                    "mode": "absolute",
                                    "steps": [
                                        {"color": "red", "value": None},
                                        {"color": "green", "value": 1}
                                    ]
                                }
                            }
                        }
                    },
                    {
                        "id": 5,
                        "title": "DAG Processing Stats",
                        "type": "graph",
                        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 13},
                        "targets": [
                            {
                                "expr": "rate(airflow_dag_processing_stats[5m])",
                                "legendFormat": "{{dag_id}}"
                            }
                        ]
                    }
                ],
                "schemaVersion": 16,
                "version": 1
            }
        }
        
        return dashboard
    
    def run_setup(self):
        """
        Run the complete Grafana setup
        """
        logger.info("Starting Grafana dashboard setup...")
        
        # Create overview dashboard
        overview_dashboard = self.create_nyc_tlc_overview_dashboard()
        self.create_dashboard(overview_dashboard['dashboard'])
        
        # Create Airflow monitoring dashboard
        airflow_dashboard = self.create_airflow_monitoring_dashboard()
        self.create_dashboard(airflow_dashboard['dashboard'])
        
        logger.info("Grafana dashboard setup completed!")


def main():
    """
    Main function to run the Grafana setup
    """
    logger.info("Starting Grafana configuration...")
    
    # Initialize and run setup
    grafana_setup = GrafanaSetup()
    grafana_setup.run_setup()


if __name__ == "__main__":
    main()