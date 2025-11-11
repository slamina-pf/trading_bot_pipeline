# Use the same Airflow version as in docker-compose
FROM apache/airflow:2.9.3

# Switch to root to install system dependencies
USER root

# Install required system packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
# Switch back to airflow user
USER airflow

# Install Python libraries from requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt