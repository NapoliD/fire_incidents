# San Francisco Fire Incidents Data Warehouse

This project implements a data warehouse solution for analyzing fire incidents in San Francisco, using data from the SF Government Open Data portal.

## Solution Overview

The solution consists of the following components:

1. **Data Warehouse**: PostgreSQL database running in a Docker container
2. **ETL Pipeline**: Python script to fetch and update data daily from the SF Government API
3. **Data Model**: Optimized schema with proper indexing for efficient querying
4. **Sample Reports**: Example queries demonstrating aggregation capabilities

## Technical Architecture

### Data Warehouse
- PostgreSQL database for storing and querying fire incidents data
- Docker container for easy deployment and portability

### ETL Pipeline
- Daily data synchronization with source API
- Data validation and transformation
- Incremental updates to maintain efficiency

### Data Model
- Fact table: fire_incidents
- Dimension tables:
  - time_dimension
  - district_dimension
  - battalion_dimension

## Setup Instructions

1. Install Docker and Docker Compose
2. Clone this repository
3. Run `docker-compose up` to start the PostgreSQL container
4. Execute the ETL script to populate initial data

## Usage

### Sample Queries

The following queries demonstrate how to aggregate incidents by different dimensions:

1. Incidents by Time Period
2. Incidents by District
3. Incidents by Battalion

## Development

### Prerequisites
- Docker
- Python 3.8+
- PostgreSQL client (optional)

### Project Structure
```
├── docker-compose.yml
├── etl/
│   ├── main.py
│   └── requirements.txt
├── sql/
│   ├── schema.sql
│   └── sample_queries.sql
└── README.md
```

## Assumptions

1. The source data is updated daily
2. The data structure remains consistent
3. The API endpoint remains stable
4. The data volume is manageable for a single PostgreSQL instance

## Future Improvements

1. Add data quality monitoring
2. Implement automated testing
3. Add visualization layer
4. Implement data partitioning for larger datasets