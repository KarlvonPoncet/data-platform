# Wiener Linien Data Platform 🚋

A real-time data platform that ingests, transforms, and visualizes U-Bahn departures from the Wiener Linien API.

## 🏗 Architecture

The platform consists of five Dockerized services:

1.  **Ingestion Service** (`backend/verkehrs-api/services/ingestion_service.py`):
    -   Fetches real-time traffic data from the Wiener Linien API every 60 seconds.
    -   Stores raw JSON responses in MinIO (`s3://traffic-data/raw/...`).

2.  **Transformation Service** (`backend/verkehrs-api/services/transformation_service.py`):
    -   Runs every 10 minutes.
    -   Reads raw data from MinIO using DuckDB and the newly built `StateManager`.
    -   Deduplicates departures (keeps the latest update for each unique trip).
    -   Filters for **U-Bahn** lines only.
    -   Saves structured Parquet files to `s3://traffic-data/refined/...`.

3.  **Feature Service** (`backend/verkehrs-api/services/feature_service.py`):
    -   Runs periodically (e.g. hourly).
    -   Reads refined Parquet files incrementally without heavy MinIO analytical queries.
    -   Computes business features casting timestamps, calculating `delay_seconds`, and adding dynamic boolean `is_delayed` flags.
    -   Stores finalized data to `s3://traffic-data/features/...`.

4.  **Dashboard** (`frontend/dashboard.py`):
    -   A Streamlit web application.
    -   Visualizes the refined data (Live Monitor, KPIs, Charts).
    -   Accessible at [http://localhost:8501](http://localhost:8501).

5.  **MinIO** (Object Storage):
    -   S3-compatible storage for raw and refined data.
    -   Console accessible at [http://localhost:9001](http://localhost:9001) (User/Pass: `minioadmin` / `minioadmin`).

## 🚀 Getting Started

### Prerequisites
-   Docker & Docker Compose

### Running the Platform

1.  Start all services:
    ```bash
    docker-compose up --build -d
    ```

2.  Access the Dashboard:
    Open **[http://localhost:8501](http://localhost:8501)** in your browser.

3.  Access MinIO Console (Optional):
    Open **[http://localhost:9001](http://localhost:9001)**.
    -   **Username**: `minioadmin`
    -   **Password**: `minioadmin`

### Stopping the Platform
```bash
docker-compose down
```

## 📂 Project Structure

```
data-platform/
├── backend/
│   ├── Dockerfile              # Docker environment for backend services
│   ├── requirements.txt        # Backend dependencies (minio, duckdb, etc.)
│   └── verkehrs-api/
│       ├── services/           # Data pipeline applications (ingestion, transformation, feature)
│       ├── core/               # Shared logic (state tracking, MinIO operations, formatting)
│       ├── clients/            # API & Reference Data clients
│       ├── scripts/            # Local dev scripts
│       └── tests/              # Pytest backend test suite
├── frontend/
│   ├── Dockerfile              # Docker environment for Streamlit
│   ├── requirements.txt        # Frontend dependencies (streamlit, plotly)
│   └── dashboard.py           # UI Code
├── docker-compose.yaml         # Orchestration
└── README.md
```
