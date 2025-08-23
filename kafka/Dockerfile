FROM python:3.9-slim

WORKDIR /app

# Install system dependencies (needed for numpy/pandas compilation)
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY producer/requirements.txt .

# Install Python dependencies
# Pin numpy and pandas to avoid binary incompatibility
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir "numpy==1.23.5" "pandas==1.5.3"

# Copy application code
COPY producer/ .

# Create data directory
RUN mkdir -p /app/data

# Set environment variables
ENV PYTHONPATH=/app
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092

# Run the producer
CMD ["python", "data_producer.py"]
