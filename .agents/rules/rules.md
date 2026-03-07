---
trigger: always_on
---

Python & Environment

    Virtual Environment Mandatory: Always verify or activate the .venv before suggesting or running any commands. All dependencies (DuckDB, APScheduler, etc.) must be installed within this environment.

    Type Hinting: Use Python type hints (List, Dict, Optional, etc.) for all function signatures to ensure maintainability in the transformation logic.

    Async Patterns: Prefer asyncio for I/O bound tasks (like scheduling or triggering MinIO requests) but keep DuckDB execution synchronous within the thread to avoid connection locking issues.

DuckDB & Data Engineering

    No "Global Globs": Never use **/*.parquet for production scans. Always prefer partitioned paths (e.g., year=2024/month=01/...) to minimize S3 LIST overhead.

    State over Scans: Do not calculate the "High-Water Mark" by scanning the refined bucket. Always refer to the StateManager (JSON/DB) to find the last processed timestamp.

    Memory Management: Always set a memory limit for DuckDB sessions (e.g., SET max_memory='4GB') to prevent the transformation process from crashing the host container.

    Idempotency: Every transformation job must be idempotent. Re-running a job for a specific timestamp should not create duplicate records in the refined layer.

Project Architecture & Modularity

    Separation of Concerns: * SQL: Keep in queries.py or .sql files.

        Logic: Keep in service.py.

        State: Keep in state.py.

    Configuration: Never hardcode credentials. Use a Config class that pulls from .env or environment variables.

    Logging: Use structured logging. Every transformation start, row count, and completion must be logged with a timestamp.

Infrastructure (MinIO/S3)

    Connection Settings: Always ensure s3_url_style='path' is set for MinIO compatibility.

    Atomic Writes: Use unique filenames for refined data (e.g., data_{timestamp}.parquet) to ensure "Append-only" logic and avoid write-collisions.

Testing

    Mocking I/O: When writing tests, mock the S3/MinIO filesystem using fsspec or local temporary directories to avoid hitting the actual dev bucket.