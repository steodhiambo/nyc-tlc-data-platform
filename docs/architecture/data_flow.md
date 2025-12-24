# Data Flow

This document describes the end-to-end data flow in the NYC TLC Data Platform, from raw data ingestion to final consumption.

## Ingestion Flow

```mermaid
graph LR
    subgraph "Data Sources"
        A[NYC TLC Public Data]
        B[Third Party APIs]
    end
    
    subgraph "Raw Layer"
        C[S3 Raw Bucket]
        D[Glue Catalog]
    end
    
    A --> C
    B --> C
    C --> D
```

### Raw Data Ingestion
Raw taxi data is ingested from multiple sources:
- Yellow taxi trip records
- Green taxi trip records
- FHV (For-Hire Vehicle) data
- Taxi zone lookup data

The data is stored in Parquet format in S3 for efficient processing.

## Processing Flow

```mermaid
graph LR
    subgraph "Processing Layer"
        A[Raw S3 Data]
        B[Extract Task]
        C[Transform Task]
        D[Load Task]
        E[Quality Check Task]
    end
    
    A --> B
    B --> C
    C --> D
    D --> E
    E --> F[Data Warehouse]
```

### ETL Process
The ETL process includes several steps:

1. **Extract**: Read data from S3 raw bucket
2. **Transform**: Clean, validate, and enrich the data
3. **Load**: Insert processed data into PostgreSQL
4. **Validate**: Run quality checks using Great Expectations

## Transformation Flow

```mermaid
graph LR
    subgraph "Staging Layer"
        A[Staging Tables]
    end
    
    subgraph "Core Layer"
        B[dbt Models]
        C[Fact Tables]
        D[Dimension Tables]
    end
    
    subgraph "Analytics Layer"
        E[Aggregated Views]
        F[ML Features]
    end
    
    A --> B
    B --> C
    B --> D
    C --> E
    D --> E
    C --> F
    D --> F
```

### dbt Transformations
dbt models transform the staging data into dimensional models:
- Fact tables containing business events
- Dimension tables containing reference data
- Aggregated views for common analytics queries

## Quality Assurance Flow

```mermaid
graph LR
    subgraph "Quality Layer"
        A[Data Validation]
        B[Great Expectations]
        C[Alerting System]
        D[Monitoring Dashboard]
    end
    
    A --> B
    B --> C
    B --> D
    C --> E[Data Team]
    D --> F[Stakeholders]
```

### Quality Checks
Data quality is enforced through:
- Schema validation
- Range checks for numeric fields
- Completeness checks for required fields
- Cross-reference validations
- Business rule validations

## Consumption Flow

```mermaid
graph LR
    subgraph "Data Warehouse"
        A[Fact Tables]
        B[Dimension Tables]
    end
    
    subgraph "Consumption Tools"
        C[BI Tools]
        D[Data Science Platform]
        E[API Services]
        F[Dashboard Tools]
    end
    
    A --> C
    B --> C
    A --> D
    B --> D
    A --> E
    B --> E
    A --> F
    B --> F
```

### End User Access
Data is consumed through multiple channels:
- Business Intelligence tools (Tableau, Power BI)
- Data science platforms (Jupyter, RStudio)
- REST APIs for application integration
- Self-service dashboards