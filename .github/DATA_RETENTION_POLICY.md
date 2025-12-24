# Data Retention Policy

## Purpose
This policy establishes guidelines for data retention and disposal within the NYC TLC Data Platform to ensure compliance with legal requirements and optimize storage costs.

## Scope
This policy applies to all data stored within the NYC TLC Data Platform, including raw data, processed data, and metadata.

## Retention Periods

### Raw Data
- Raw NYC TLC trip data: 7 years (compliance requirement)
- Raw log files: 1 year
- Temporary processing files: 30 days

### Processed Data
- Core dimensional models: 7 years (compliance requirement)
- Aggregated analytics data: 5 years
- Temporary staging data: 90 days

### Metadata
- Data lineage information: 10 years
- Data quality logs: 3 years
- Access logs: 3 years
- Audit logs: 7 years

## Data Lifecycle Management

### Storage Tiers
- Hot data (frequently accessed): Standard storage
- Warm data (occasionally accessed): Standard_IA storage
- Cold data (rarely accessed): Glacier storage
- Archive data: Deep Archive storage

### Lifecycle Rules
- Data automatically transitions to lower-cost storage after specified periods
- Data is deleted automatically after retention period expires
- Legal holds can extend retention periods

## Data Disposal

### Disposal Process
- Data disposal must be approved by data owner
- Disposal must follow secure deletion procedures
- Disposal must be documented and audited

### Exceptions
- Data subject to legal hold must be retained regardless of retention period
- Data required for ongoing analysis may have extended retention
- Historical data for research purposes may have extended retention

## Compliance

### Legal Requirements
- Must comply with NYC records retention laws
- Must comply with federal transportation data requirements
- Must comply with privacy regulations

### Audit Requirements
- Retention compliance must be audited annually
- Disposal activities must be logged and reviewed
- Retention exceptions must be documented and approved

## Responsibilities

### Data Governance Team
- Monitor compliance with retention policies
- Manage automated retention and disposal processes
- Maintain retention policy documentation

### Data Owners
- Ensure their data follows retention requirements
- Request retention exceptions when needed
- Approve data disposal activities

### Data Engineering Team
- Implement automated retention and disposal
- Maintain disposal procedures and tools
- Monitor storage costs and optimization

## Enforcement
Non-compliance with this policy may result in:
- Process suspension until compliance is achieved
- Additional monitoring and controls
- Disciplinary action for willful violations

## Review
This policy will be reviewed annually or when legal requirements change.