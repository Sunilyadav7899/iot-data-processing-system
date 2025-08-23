FROM bitnami/spark:3.4.1

USER root

# Install Python and system dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    curl \
    wget \
    netcat \
    && rm -rf /var/lib/apt/lists/*


# Set working directory
WORKDIR /app

# Copy requirements and install Python packages
COPY streaming/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Download required JAR files for Kafka and PostgreSQL
RUN wget -O /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar

RUN wget -O /opt/bitnami/spark/jars/kafka-clients-3.4.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar

RUN wget -O /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.1/spark-token-provider-kafka-0-10_2.12-3.4.1.jar

RUN wget -O /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

RUN wget -O /opt/bitnami/spark/jars/postgresql-42.6.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Copy application files
COPY streaming/ .
COPY jobs/ ./jobs/

# Create directories
RUN mkdir -p /app/checkpoints /app/logs

# Set permissions
RUN chown -R 1001:1001 /app

# Switch back to non-root user
USER 1001

# Set environment variables
ENV PYTHONPATH=/app
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Default command
CMD ["bash", "/app/jobs/submit_jobs.sh"]