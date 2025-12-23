# NYC TLC Data Platform - Troubleshooting Guide

This guide provides detailed steps for troubleshooting common issues in the NYC TLC Data Platform.

## Table of Contents
1. [Common Issues and Solutions](#common-issues)
2. [Monitoring and Diagnostics](#monitoring-diagnostics)
3. [Performance Optimization](#performance-optimization)
4. [Data Quality Issues](#data-quality-issues)
5. [Infrastructure Problems](#infrastructure-problems)
6. [Recovery Procedures](#recovery-procedures)

## Common Issues {#common-issues}

### 1. Pipeline Failures

#### Symptoms:
- Airflow DAGs failing or getting stuck
- Missing data in the data warehouse
- Alert notifications about pipeline failures

#### Diagnosis:
```bash
# Check recent pipeline logs
python scripts/troubleshooting.py troubleshoot

# Check Airflow logs
docker-compose logs webserver | grep -i error
docker-compose logs scheduler | grep -i error
```

#### Solutions:
1. Check S3 connectivity and permissions
2. Verify database connection
3. Review pipeline logs for specific error messages
4. Check resource availability (CPU, memory, disk space)
5. Retry failed tasks from Airflow UI

### 2. Data Quality Issues

#### Symptoms:
- Invalid or null values in processed data
- Data inconsistencies
- Quality metric alerts

#### Diagnosis:
```bash
# Check data quality metrics
python scripts/troubleshooting.py troubleshoot
```

#### Solutions:
1. Review data validation rules
2. Check source data for anomalies
3. Update data transformation logic if needed
4. Implement additional data quality checks

### 3. Resource Bottlenecks

#### Symptoms:
- Slow pipeline execution
- High CPU/memory usage
- Database connection timeouts

#### Diagnosis:
```bash
# Check system resource usage
docker stats

# Monitor database connections
# Connect to PostgreSQL and run:
# SELECT count(*) FROM pg_stat_activity;
```

#### Solutions:
1. Scale up infrastructure resources
2. Optimize queries and transformations
3. Implement data partitioning
4. Add more Airflow workers

## Monitoring and Diagnostics {#monitoring-diagnostics}

### System Health Checks

#### Check all services:
```bash
docker-compose ps
```

#### Check Airflow health:
```bash
curl http://localhost:8080/health
```

#### Check database connectivity:
```bash
# Test database connection
python -c "from sqlalchemy import create_engine; engine = create_engine('postgresql://nyc_tlc_user:nyc_tlc_password@localhost:5433/nyc_tlc_dw'); engine.execute('SELECT 1')"
```

### Log Analysis

#### Airflow logs:
```bash
# View scheduler logs
docker-compose logs scheduler

# View webserver logs
docker-compose logs webserver

# View worker logs
docker-compose logs worker
```

#### Application logs:
```bash
# Check troubleshooting logs
tail -f troubleshooting.log

# Check monitoring logs
tail -f monitoring.log
```

## Performance Optimization {#performance-optimization}

### Database Optimization

#### Indexes:
Ensure proper indexes exist for common queries:
```sql
-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_yellow_tripdata_pickup_datetime 
ON taxi_data.yellow_tripdata (pickup_datetime);

CREATE INDEX IF NOT EXISTS idx_yellow_tripdata_dropoff_datetime 
ON taxi_data.yellow_tripdata (dropoff_datetime);

CREATE INDEX IF NOT EXISTS idx_yellow_tripdata_pickup_location_id 
ON taxi_data.yellow_tripdata (pickup_location_id);

CREATE INDEX IF NOT EXISTS idx_yellow_tripdata_dropoff_location_id 
ON taxi_data.yellow_tripdata (dropoff_location_id);
```

#### Query Optimization:
- Use appropriate WHERE clauses
- Limit result sets when possible
- Use EXPLAIN ANALYZE to understand query performance

### Pipeline Optimization

#### Parallel Processing:
- Increase Airflow worker count
- Optimize task dependencies
- Use appropriate execution timeouts

#### Data Processing:
- Use batch processing for large datasets
- Implement incremental loading
- Optimize data transformation logic

## Data Quality Issues {#data-quality-issues}

### Common Data Quality Problems

#### Invalid Date/Time Values:
- Check for null or invalid datetime values
- Validate timezone handling
- Implement proper date validation

#### Missing Location Data:
- Verify taxi zone lookup table integrity
- Handle missing location IDs gracefully
- Implement fallback location mapping

#### Negative Values:
- Validate fare amounts, distances, passenger counts
- Implement range checks during transformation
- Log and handle outliers appropriately

### Data Validation Rules

#### Implemented Checks:
1. Null pickup/dropoff datetime validation
2. Invalid coordinate validation
3. Negative fare/amount validation
4. Passenger count validation
5. Trip distance validation
6. Location ID validation against zone lookup

#### Custom Validation:
Add custom validation rules as needed for specific business requirements.

## Infrastructure Problems {#infrastructure-problems}

### Docker Container Issues

#### Restart containers:
```bash
docker-compose down
docker-compose up -d
```

#### Check container resources:
```bash
docker stats
```

#### View container logs:
```bash
docker-compose logs <service-name>
```

### AWS Resource Issues

#### S3 Bucket Problems:
- Check IAM permissions
- Verify bucket policies
- Monitor bucket size and request rates

#### RDS Issues:
- Monitor connection limits
- Check storage space
- Review query performance

### Network Connectivity

#### Test connectivity:
```bash
# Test S3 connectivity
aws s3 ls s3://<your-bucket-name>/

# Test database connectivity
psql -h localhost -p 5433 -U nyc_tlc_user -d nyc_tlc_dw
```

## Recovery Procedures {#recovery-procedures}

### Pipeline Recovery

#### For Failed DAGs:
1. Identify the failed task from Airflow UI
2. Check the task logs for error details
3. Fix the underlying issue
4. Clear the failed task
5. Re-run the task or DAG

#### For Data Corruption:
1. Identify affected data range
2. Remove corrupted data from target tables
3. Re-run the relevant pipeline(s)
4. Verify data integrity after recovery

### Database Recovery

#### Backup and Restore:
```bash
# Create a backup
pg_dump -h localhost -p 5433 -U nyc_tlc_user nyc_tlc_dw > backup.sql

# Restore from backup
psql -h localhost -p 5433 -U nyc_tlc_user nyc_tlc_dw < backup.sql
```

#### Data Validation After Recovery:
- Run data quality checks
- Verify record counts
- Check for data consistency

### SLA Compliance Recovery

#### For Missed SLAs:
1. Document the SLA violation
2. Identify root cause
3. Implement preventive measures
4. Update monitoring for early detection
5. Report to stakeholders if required

## Emergency Procedures

### Critical System Down
1. Assess the scope of the issue
2. Notify on-call team immediately
3. Follow incident response procedures
4. Document the incident
5. Conduct post-mortem analysis

### Data Loss
1. Stop all data processing immediately
2. Assess the extent of data loss
3. Restore from backups if possible
4. Re-process lost data
5. Implement additional safeguards

## Monitoring and Alerting

### Key Metrics to Watch:
- Pipeline execution success rate
- Data processing volume
- System resource utilization
- Data quality metrics
- SLA compliance metrics

### Alert Thresholds:
- Pipeline failure rate > 5%
- Data processing delay > 1 hour
- System CPU usage > 80%
- Data quality issues > 1%

## Best Practices

### Preventive Measures:
- Regular monitoring and maintenance
- Automated testing of data pipelines
- Proper error handling and logging
- Regular backups
- Performance monitoring

### Documentation:
- Maintain up-to-date runbooks
- Document all processes and procedures
- Keep infrastructure documentation current
- Regular knowledge sharing sessions

---

For additional support, contact the Data Engineering team at data-team@example.com