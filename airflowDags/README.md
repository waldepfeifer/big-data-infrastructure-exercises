# Airflow DAGs Project

This project contains Apache Airflow DAGs for processing aircraft-related data. It includes three main DAGs that handle different aspects of aircraft data processing and ingestion.

## Prerequisites

Before you begin, ensure you have the following installed:
- Docker and Docker Compose
- Python 3.8 or higher
- Git
- AWS CLI (for S3 access)
- PostgreSQL client tools

## Project Structure

```
.
├── dags/                  # Contains Airflow DAG definitions
│   ├── adsbexchange_ingest_dag.py        # Processes ADS-B exchange data
│   ├── aircrafts_database_ingest_dag.py  # Handles aircraft database updates
│   └── fuel_consumption_ingest_dag.py    # Processes fuel consumption data
├── include/              # Additional files to be included in the project
├── plugins/              # Custom Airflow plugins
├── tests/                # Test files for DAGs
├── .env                  # Environment variables (create this file in the root directory)
├── airflow_settings.yaml # Airflow configuration
├── docker-compose.yml    # Docker Compose configuration
├── Dockerfile           # Custom Docker image configuration
├── requirements.txt     # Python dependencies
└── README.md           # This file
```

## Available DAGs

### 1. ADS-B Exchange Ingest DAG
- **Purpose**: Processes aircraft tracking data from ADS-B Exchange
- **Schedule**: Daily
- **Features**:
  - Downloads and processes JSON data
  - Transforms data to Parquet format
  - Uploads to S3 in bronze/silver layers
  - Loads data into PostgreSQL

### 2. Aircraft Database Ingest DAG
- **Purpose**: Updates aircraft database information
- **Schedule**: Daily
- **Features**:
  - Downloads aircraft database
  - Validates file integrity
  - Processes and transforms data
  - Updates PostgreSQL database

### 3. Fuel Consumption Ingest DAG
- **Purpose**: Processes aircraft fuel consumption data
- **Schedule**: One-time execution
- **Features**:
  - Downloads fuel consumption rates
  - Transforms JSON data
  - Updates PostgreSQL database

## Environment Setup

### Environment Variables (.env)

Create a `.env` file in the root directory with the following variables:

```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_SESSION_TOKEN=your-session-token  # If using temporary credentials
S3_BUCKET=your-bucket-name

# Database Configuration
BDI_DB_HOST=your-db-host
BDI_DB_PORT=5432
BDI_DB_USERNAME=your-db-username
BDI_DB_PASSWORD=your-db-password
BDI_DB_NAME=your-db-name

# Airflow Core Settings
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://${BDI_DB_USERNAME}:${BDI_DB_PASSWORD}@${BDI_DB_HOST}:${BDI_DB_PORT}/${BDI_DB_NAME}
```

Important notes about the `.env` file:
1. Never commit the `.env` file to version control
2. Keep a `.env.example` file with placeholder values for reference
3. Ensure AWS credentials have appropriate S3 and RDS access
4. Database credentials should have read/write access to the specified database

## Getting Started

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-name>
   ```

2. Set up environment variables:
   - Create a `.env` file in the root directory
   - Copy the contents from the Environment Variables section above
   - Update the values with your specific configurations
   - Ensure the file is in the correct location (root directory)

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Start the Airflow environment:
   ```bash
   docker-compose up -d
   ```

5. Access the Airflow UI:
   - Open your browser and go to `http://localhost:8080`
   - Default credentials:
     - Username: `admin`
     - Password: `admin`

## Development

- Place new DAGs in the `dags/` directory
- Add custom plugins to the `plugins/` directory
- Update `requirements.txt` for new Python dependencies
- Use `airflow_settings.yaml` for local development configurations

## Testing

Run tests using:
```bash
pytest tests/
```

## Deployment

To deploy to Astronomer:
1. Install the Astronomer CLI:
   ```bash
   curl -sSL https://install.astronomer.io | sudo bash
   ```

2. Login to Astronomer:
   ```bash
   astro auth login
   ```

3. Deploy your project:
   ```bash
   astro deploy
   ```

## Troubleshooting

Common issues and solutions:
- Port conflicts: If port 8080 is in use, modify `docker-compose.yml`
- Database issues: Check PostgreSQL logs in Docker
- DAG loading problems: Check Airflow logs in the UI
- Environment variable issues: Ensure `.env` file is in the correct location and all required variables are set
- AWS credential issues: Verify AWS credentials and permissions
- S3 access issues: Check bucket permissions and credentials

## Support

For issues and feature requests, please open an issue in the repository.

## License

[Add your license information here]
