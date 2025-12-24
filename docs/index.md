# NYC TLC Data Platform

Welcome to the NYC Taxi and Limousine Commission (TLC) Data Platform documentation. This platform processes and analyzes taxi trip data from NYC, providing valuable insights for city planning, transportation analysis, and research.

## Overview

The NYC TLC Data Platform is a comprehensive data engineering solution that:

- Ingests raw taxi trip data from multiple sources
- Applies data quality checks and transformations
- Implements robust data governance and lineage tracking
- Provides analytical capabilities through dimensional modeling
- Ensures compliance with data governance policies

## Architecture

```mermaid
graph TB
    subgraph "Data Sources"
        A[Yellow Taxi Data] 
        B[Green Taxi Data]
        C[FHV Data]
    end
    
    subgraph "Ingestion Layer"
        D[S3 Raw Bucket]
        E[Glue Crawler]
    end
    
    subgraph "Processing Layer"
        F[Apache Airflow]
        G[ETL Pipelines]
        H[Great Expectations]
    end
    
    subgraph "Storage Layer"
        I[Staging Tables]
        J[Core Data Warehouse]
        K[Data Lake Parquet]
    end
    
    subgraph "Analytics Layer"
        L[dbt Models]
        M[Fact Tables]
        N[Dimension Tables]
    end
    
    subgraph "Consumption Layer"
        O[BI Tools]
        P[Data Science]
        Q[API Layer]
    end
    
    A --> D
    B --> D
    C --> D
    D --> E
    E --> F
    F --> G
    G --> H
    G --> I
    I --> J
    I --> K
    J --> L
    L --> M
    L --> N
    M --> O
    M --> P
    M --> Q
    N --> O
    N --> P
    N --> Q
```

## Key Features

- **Data Quality**: Automated validation using Great Expectations
- **Governance**: Comprehensive lineage tracking and metadata management
- **Scalability**: Cloud-native architecture using AWS services
- **Monitoring**: Real-time alerting and monitoring capabilities
- **Compliance**: Adherence to data governance policies

## Getting Started

Check out our [Development Setup](development/setup.md) guide to get started with the platform.