# SF Fire Incidents ETL Project

This repository provides a complete ETL solution to ingest, transform, and validate fire incident data for the City of San Francisco. It includes:

* **Database schema** (`database_schema.py`) defining dimensions, staging, and fact tables.
* **Interactive ETL script** (`etl_fire.py`) that prompts for the CSV path.
* **Stable ETL script** (`etl_fire_estable.py`) that runs without user interaction.
* **Validation report generator** (`report.py`) creating metrics and visualizations.
* **Docker Compose** configuration (`docker-compose.yml`) for local PostgreSQL setup.
* **Future Airflow DAG** (`dags/fire_etl_dag.py`) for scheduling and orchestration.

---

## Table of Contents

1. [Project Structure](#project-structure)
2. [Requirements](#requirements)
3. [Installation](#installation)
4. [Configuration](#configuration)

   * [Environment Variables](#environment-variables)
5. [Usage](#usage)

   * [Run Interactive ETL](#run-interactive-etl)
   * [Run Stable ETL](#run-stable-etl)
   * [Generate Validation Report](#generate-validation-report)
   * [Docker Setup](#docker-setup)
6. [Airflow Orchestration (Future)](#airflow-orchestration-future)
7. [Publishing to GitHub](#publishing-to-github)
8. [Contributing](#contributing)
9. [License](#license)

---

## Project Structure

```plaintext

├── etl
  ── etl_fire.py              # ETL script with CSV path prompt
  ── etl_fire_estable.py      # ETL script without prompt (stable)
  ── report.py                # Validation report generator
  ── database_schema.py        # Schema definition for Data Warehouse
├── docker-compose.yml       # Local PostgreSQL setup via Docker
├── dags/
│   └── fire_etl_dag.py      # Airflow DAG for future orchestration
├── requirements.txt         # Python dependencies
└── README.md                # This documentation
```

---

## Requirements

* Python 3.8 or higher
* PostgreSQL (or any DB supported by SQLAlchemy)
* Docker and Docker Compose (for the local database)
* pip (to install Python packages)

---

## Installation

1. **Clone the repository**

   ```bash
   git clone https://github.com/<YOUR_USERNAME>/sf-fire-incidents-etl.git
   cd sf-fire-incidents-etl
   ```

2. **Set up a Python virtual environment and install dependencies**

   ```bash
   python -m venv venv
   source venv/bin/activate      # Linux/macOS
   venv\Scripts\activate       # Windows

   pip install --upgrade pip
   pip install -r requirements.txt
   ```

---

## Configuration

### Environment Variables

| Variable         | Description                                   | Default                                                           |
| ---------------- | --------------------------------------------- | ----------------------------------------------------------------- |
| `FIRE_DW_DB_URI` | Database URI for SQLAlchemy connection        | `postgresql://postgres:postgres@localhost:5432/sf_fire_incidents` |
| `FIRE_CSV_PATH`  | Path to the input CSV file                    | `./Fire_Incidents_YYYYMMDD.csv`                                   |
| `REPORT_PATH`    | Output path for the validation report (Excel) | `validation_report.xlsx`                                          |

You can define them in a `.env` file or export directly:

```bash
export FIRE_DW_DB_URI="postgresql://user:pass@host:port/dbname"
export FIRE_CSV_PATH="/path/to/Fire_Incidents.csv"
export REPORT_PATH="./reports/validation_report.xlsx"
```

---

## Usage

### Run Interactive ETL

This script prompts for the CSV path or uses `FIRE_CSV_PATH`:

```bash
python etl_fire.py
```

### Run Stable ETL

Runs without prompts, using `FIRE_CSV_PATH`:

```bash
python etl_fire_estable.py
```

### Generate Validation Report

Generates descriptive metrics and saves an Excel report:

```bash
python report.py
```

### Docker Setup

Use Docker Compose to spin up a local PostgreSQL instance:

```bash
docker-compose up -d
```

* The service `postgres` runs on port `5432`.
* Initialization SQL scripts can be placed in `./sql/` and will be executed on startup. citeturn1file0

To tear down:

```bash
docker-compose down
```

---

## Airflow Orchestration (Future)

A DAG for Airflow is provided in `dags/fire_etl_dag.py` for scheduling the ETL daily at 02:00 AM:

```python
# dags/fire_etl_dag.py
from airflow import DAG
# ...
with DAG(
    dag_id="fire_incident_etl",
    schedule_interval="0 2 * * *",
    start_date=datetime(2025, 5, 15),
    catchup=False,
) as dag:
    PythonOperator(
        task_id="run_etl",
        python_callable=run_etl,
    )
```

Place this file in your Airflow `dags/` directory and configure the `fire_dw_db` connection in Airflow Admin. citeturn1file1

---

## Publishing to GitHub

1. **Initialize Git** (if not already):

   ```bash
   git init
   ```

2. **Create a `.gitignore`** with:

   ```gitignore
   venv/
   __pycache__/
   *.py[cod]
   .env
   *.sqlite3
   .DS_Store
   ```

3. **Commit and push**:

   ```bash
   git add .
   git commit -m "Initial ETL project setup"
   git remote add origin https://github.com/<YOUR_USERNAME>/sf-fire-incidents-etl.git
   git push -u origin main
   ```

---

## Contributing

Contributions are welcome! Please open issues or pull requests for enhancements and bug fixes.

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
