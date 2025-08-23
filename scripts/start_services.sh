#!/bin/bash
# Start all IoT Data Processing System services

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=== Starting IoT Data Processing System ===${NC}"

# Stop any existing containers
echo "Stopping any existing containers..."
docker-compose down -v 2>/dev/null || true

# Create networks and volumes (no start yet)
echo "Creating Docker networks and volumes..."
docker-compose up --no-start

# Helper: wait a few secs for service to initialize
wait_a_bit() {
    local service=$1
    local seconds=${2:-10}
    echo "⏳ Waiting $seconds seconds for $service to initialize..."
    sleep $seconds
}

# === Infrastructure services ===
echo -e "\n${BLUE}Starting infrastructure services...${NC}"

echo "Starting Zookeeper..."
docker-compose up -d zookeeper
wait_a_bit "Zookeeper" 10

echo "Starting Kafka..."
docker-compose up -d kafka
wait_a_bit "Kafka" 15

echo "Starting Hadoop Namenode..."
docker-compose up -d namenode
wait_a_bit "Namenode" 10

echo "Starting Hadoop Datanode..."
docker-compose up -d datanode
wait_a_bit "Datanode" 5

echo "Starting PostgreSQL..."
docker-compose up -d postgres
wait_a_bit "PostgreSQL" 10

echo "Starting Redis..."
docker-compose up -d redis
wait_a_bit "Redis" 5

# === Spark cluster ===
echo -e "\n${BLUE}Starting Spark cluster...${NC}"

echo "Starting Spark Master..."
docker-compose up -d spark-master
wait_a_bit "Spark Master" 10

echo "Starting Spark Worker..."
docker-compose up -d spark-worker
wait_a_bit "Spark Worker" 10

# === Grafana ===
echo -e "\n${BLUE}Starting Grafana...${NC}"
docker-compose up -d grafana
wait_a_bit "Grafana" 10

# === Processing services ===
echo -e "\n${BLUE}Starting data processing services...${NC}"

echo "Starting Kafka Producer..."
docker-compose up -d kafka-producer
wait_a_bit "Kafka Producer" 5

echo "Starting Spark Streaming..."
docker-compose up -d spark-streaming
wait_a_bit "Spark Streaming" 10

# Final status
echo -e "\n${BLUE}Final service status check...${NC}"
docker-compose ps

# Service URLs
echo -e "\n${GREEN}=== Services Started Successfully! ===${NC}"
echo -e "\n${BLUE}Service URLs:${NC}"
echo "• Grafana Dashboard: http://localhost:3000 (admin/admin)"
echo "• Spark Master UI: http://localhost:8080"
echo "• Spark Worker UI: http://localhost:8081"
echo "• Hadoop Namenode UI: http://localhost:9870"
echo "• PostgreSQL: localhost:5432 (iot_user/iot_password)"
echo "• Redis: localhost:6379"
echo
echo -e "${BLUE}Kafka Topics:${NC}"
echo "• iot-temperature: Temperature sensor data"
echo "• iot-window-events: Window sensor events"
echo

# Quick logs for debugging
echo -e "\n${BLUE}Recent logs from key services:${NC}"
echo -e "${YELLOW}--- Kafka Producer ---${NC}"
docker-compose logs --tail=5 kafka-producer 2>/dev/null || echo "No logs yet"
echo -e "${YELLOW}--- Spark Streaming ---${NC}"
docker-compose logs --tail=5 spark-streaming 2>/dev/null || echo "No logs yet"

echo
echo -e "${GREEN}System startup complete!${NC}"
echo "Use 'docker-compose logs -f <service>' to monitor specific services"
echo "Use './scripts/stop_services.sh' to stop all services"
