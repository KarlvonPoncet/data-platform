FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire verkehrs-api directory content into /app
COPY verkehrs-api/ /app/

# Environment variables for default connection (can be overridden)
ENV MINIO_ENDPOINT=minio:9000
ENV MINIO_ACCESS_KEY=minioadmin
ENV MINIO_SECRET_KEY=minioadmin
ENV PYTHONPATH=/app

# Default command (will be overridden in docker-compose)
CMD ["python", "services/ingestion_service.py"]
