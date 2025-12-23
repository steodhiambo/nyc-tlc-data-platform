#!/usr/bin/env python3
"""
PostgreSQL Query Performance Analysis with pgBadger

This script automates the setup and analysis of PostgreSQL query logs using pgBadger,
generating detailed reports on query performance, slow queries, and optimization opportunities.
"""

import os
import subprocess
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Optional


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pgbadger_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PgBadgerAnalyzer:
    """
    Analyzes PostgreSQL logs using pgBadger to identify performance issues
    """
    
    def __init__(self, log_directory: str = "/var/log/postgresql", 
                 output_directory: str = "/tmp/pgbadger_reports"):
        """
        Initialize the PgBadger analyzer
        
        Args:
            log_directory: Directory containing PostgreSQL logs
            output_directory: Directory for pgBadger reports
        """
        self.log_directory = Path(log_directory)
        self.output_directory = Path(output_directory)
        self.output_directory.mkdir(exist_ok=True)
        
        # Verify pgBadger is installed
        if not self._is_pgbadger_installed():
            logger.error("pgBadger is not installed. Please install it first: apt-get install pgbadger")
            raise RuntimeError("pgBadger not found")
    
    def _is_pgbadger_installed(self) -> bool:
        """
        Check if pgBadger is installed
        
        Returns:
            True if pgBadger is installed, False otherwise
        """
        try:
            result = subprocess.run(['pgbadger', '--version'], 
                                  capture_output=True, text=True, check=True)
            logger.info(f"pgBadger found: {result.stdout.strip()}")
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False
    
    def generate_report(self, start_date: Optional[datetime] = None, 
                       end_date: Optional[datetime] = None) -> str:
        """
        Generate a pgBadger report for the specified date range
        
        Args:
            start_date: Start date for analysis (default: yesterday)
            end_date: End date for analysis (default: today)
            
        Returns:
            Path to the generated report
        """
        if start_date is None:
            start_date = datetime.now() - timedelta(days=1)
        if end_date is None:
            end_date = datetime.now()
        
        # Format dates for pgBadger
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        # Find log files for the date range
        log_files = []
        for log_file in self.log_directory.glob('postgresql-*.log*'):
            # Extract date from filename
            if start_str in log_file.name or end_str in log_file.name:
                log_files.append(str(log_file))
        
        if not log_files:
            logger.warning(f"No log files found for dates {start_str} to {end_str}")
            # Use all available logs as fallback
            log_files = [str(f) for f in self.log_directory.glob('postgresql-*.log*')]
            if not log_files:
                raise FileNotFoundError("No PostgreSQL log files found")
        
        # Generate report
        report_path = self.output_directory / f"pgbadger_report_{start_str}_to_{end_str}.html"
        
        cmd = [
            'pgbadger',
            '-f', 'stderr',  # PostgreSQL stderr format
            '-o', str(report_path),
            '--prefix="%t [%p]: [%l-1] user=%u,db=%d,app=%a,client=%h "',  # Match our log format
            '--top', '20',  # Top 20 slowest queries
            '--top-duration', '20',  # Top 20 longest duration queries
            '--top-client', '10',  # Top 10 client hosts
            '--top-database', '10',  # Top 10 databases
            '--top-user', '10',  # Top 10 users
            '--sample', '1000',  # Sample 1000 queries for detailed analysis
            *log_files
        ]
        
        try:
            logger.info(f"Generating pgBadger report: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.info(f"pgBadger report generated: {report_path}")
            logger.debug(f"pgBadger output: {result.stdout}")
            return str(report_path)
        except subprocess.CalledProcessError as e:
            logger.error(f"Error generating pgBadger report: {e}")
            logger.error(f"pgBadger stderr: {e.stderr}")
            raise
    
    def extract_performance_metrics(self, report_path: str) -> Dict:
        """
        Extract key performance metrics from pgBadger report
        
        Args:
            report_path: Path to the pgBadger HTML report
            
        Returns:
            Dictionary with performance metrics
        """
        metrics = {
            'total_queries': 0,
            'slow_queries': 0,
            'total_duration': 0,
            'avg_duration': 0,
            'top_slow_queries': [],
            'error_count': 0,
            'top_databases': [],
            'top_users': [],
            'analysis_date': datetime.now().isoformat()
        }
        
        # Read the report file to extract metrics
        try:
            with open(report_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Extract basic metrics using regex or string search
            # This is a simplified approach - in practice, you might want to parse the HTML properly
            import re
            
            # Extract total queries
            total_match = re.search(r'Total queries analyzed:\s*(\d+)', content)
            if total_match:
                metrics['total_queries'] = int(total_match.group(1))
            
            # Extract slow queries
            slow_match = re.search(r'Slow queries:\s*(\d+)', content)
            if slow_match:
                metrics['slow_queries'] = int(slow_match.group(1))
            
            # Extract total duration (this would need to be parsed from the detailed stats)
            duration_match = re.search(r'Total duration:\s*([0-9.]+\s*[a-z]+)', content, re.IGNORECASE)
            if duration_match:
                metrics['total_duration'] = duration_match.group(1)
            
            logger.info(f"Extracted metrics: {metrics}")
            return metrics
            
        except Exception as e:
            logger.error(f"Error extracting metrics from report: {e}")
            return metrics
    
    def analyze_slow_queries(self, log_directory: str = None) -> List[Dict]:
        """
        Analyze logs to identify slow queries without full pgBadger report
        
        Args:
            log_directory: Directory containing PostgreSQL logs (optional)
            
        Returns:
            List of slow queries with details
        """
        if log_directory:
            log_dir = Path(log_directory)
        else:
            log_dir = self.log_directory
        
        slow_queries = []
        
        # Find log files
        log_files = list(log_dir.glob('postgresql-*.log*'))
        
        for log_file in log_files:
            try:
                with open(log_file, 'r') as f:
                    lines = f.readlines()
                
                for line in lines:
                    # Look for duration information in logs
                    if 'duration:' in line and 'ms' in line:
                        # Extract duration and query
                        import re
                        duration_match = re.search(r'duration: ([\d.]+) ms', line)
                        if duration_match:
                            duration = float(duration_match.group(1))
                            if duration > 1000:  # More than 1 second
                                # Extract the query part (simplified)
                                query_start = line.find('statement: ') + len('statement: ')
                                if query_start > len('statement: ') - 1:
                                    query = line[query_start:].strip()
                                    slow_queries.append({
                                        'timestamp': line.split(' ')[0] if line.split(' ') else '',
                                        'duration_ms': duration,
                                        'query': query,
                                        'log_file': str(log_file)
                                    })
            except Exception as e:
                logger.warning(f"Error reading log file {log_file}: {e}")
                continue
        
        # Sort by duration (descending)
        slow_queries.sort(key=lambda x: x['duration_ms'], reverse=True)
        
        logger.info(f"Found {len(slow_queries)} slow queries (>1000ms)")
        return slow_queries[:50]  # Return top 50 slowest queries


def setup_postgresql_logging():
    """
    Provides configuration for PostgreSQL logging to enable proper pgBadger analysis
    """
    config_text = """
# Essential PostgreSQL configuration for query auditing and performance analysis
log_statement = 'all'
log_min_duration_statement = 0  # Log all statements for complete analysis
log_line_prefix = '%t [%p]: [%l-1] user=%u,db=%d,app=%a,client=%h '
log_duration = on
log_checkpoints = on
log_connections = on
log_disconnections = on
log_lock_waits = on
log_temp_files = 0
track_activities = on
track_counts = on
track_io_timing = on
    """
    
    print("PostgreSQL Configuration for Performance Auditing:")
    print(config_text)
    print("\nTo apply these settings:")
    print("1. Add these settings to postgresql.conf")
    print("2. Restart PostgreSQL or reload configuration")
    print("3. Ensure log directory is writable by PostgreSQL process")


def main():
    """
    Main function to run pgBadger analysis
    """
    parser = argparse.ArgumentParser(description='PostgreSQL Performance Analysis with pgBadger')
    parser.add_argument('--log-dir', default='/var/log/postgresql', 
                       help='Directory containing PostgreSQL logs')
    parser.add_argument('--output-dir', default='/tmp/pgbadger_reports',
                       help='Directory for pgBadger reports')
    parser.add_argument('--start-date', type=str,
                       help='Start date for analysis (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str,
                       help='End date for analysis (YYYY-MM-DD)')
    parser.add_argument('--analyze-slow', action='store_true',
                       help='Analyze slow queries without full report')
    
    args = parser.parse_args()
    
    try:
        analyzer = PgBadgerAnalyzer(args.log_dir, args.output_dir)
        
        if args.analyze_slow:
            # Analyze slow queries only
            slow_queries = analyzer.analyze_slow_queries()
            print(f"\nTop {len(slow_queries)} Slow Queries Found:")
            for i, query_info in enumerate(slow_queries[:10], 1):
                print(f"{i}. Duration: {query_info['duration_ms']}ms")
                print(f"   Query: {query_info['query'][:100]}...")
                print(f"   Time: {query_info['timestamp']}")
                print()
        else:
            # Generate full report
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d') if args.start_date else None
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d') if args.end_date else None
            
            report_path = analyzer.generate_report(start_date, end_date)
            print(f"pgBadger report generated: {report_path}")
            
            # Extract metrics
            metrics = analyzer.extract_performance_metrics(report_path)
            print(f"Performance metrics: {metrics}")
    
    except Exception as e:
        logger.error(f"Error in pgBadger analysis: {e}")
        print(f"Error: {e}")


if __name__ == "__main__":
    main()