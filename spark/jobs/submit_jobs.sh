#!/bin/bash
# Job submission script for Spark streaming applications

set -e

echo "Starting Spark job submission..."

# Wait for Spark master to be ready
echo "Waiting for Spark master to be ready..."
while ! curl -f http://spark-master:8080 > /dev/null 2>&1; do
    echo "Waiting for Spark master..."
    sleep 5
done

echo "Spark master is ready!"

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
while ! (echo > /dev/tcp/kafka/29092) >/dev/null 2>&1; do
    echo "Waiting for Kafka..."
    sleep 5
done
echo "Kafka is ready!"



# Submit temperature processing job
echo "Submitting temperature processing job..."
spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 2g \
    --executor-cores 2 \
    --num-executors 2 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0 \
    --conf spark.sql.streaming.forceDeleteTempCheckpointLocation=true \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    /app/temperature_processor.py &

TEMP_PID=$!

# Submit window event processing job
echo "Submitting window event processing job..."
spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 1g \
    --executor-cores 1 \
    --num-executors 1 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0 \
    --conf spark.sql.streaming.forceDeleteTempCheckpointLocation=true \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    /app/window_processor.py &

WINDOW_PID=$!

echo "Both streaming jobs submitted successfully!"
echo "Temperature processor PID: $TEMP_PID"
echo "Window processor PID: $WINDOW_PID"

# Function to handle cleanup
cleanup() {
    echo "Received termination signal. Cleaning up..."
    kill $TEMP_PID $WINDOW_PID 2>/dev/null || true
    wait $TEMP_PID $WINDOW_PID 2>/dev/null || true
    echo "Cleanup complete."
    exit 0
}

# Set up signal handlers
trap cleanup SIGTERM SIGINT

# Wait for both jobs
wait $TEMP_PID $WINDOW_PID

echo "All jobs completed."
