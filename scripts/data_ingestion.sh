#!/bin/bash
"""
Data ingestion script for IoT Data Processing System
Monitors and manages data flow through the system
"""

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== IoT Data Ingestion Monitor ===${NC}"

# Function to check if Kafka topics exist
check_kafka_topics() {
    echo "Checking Kafka topics..."
    
    # Wait for Kafka to be ready
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
            break
        else
            echo -n "Waiting for Kafka to be ready..."
            sleep 2
            attempt=$((attempt + 1))
        fi
    done
    
    if [ $attempt -gt $max_attempts ]; then
        echo -e "${RED}✗ Kafka not ready after waiting${NC}"
        return 1
    fi
    
    # List current topics
    echo "Current Kafka topics:"
    docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null || true
    
    # Create topics if they don't exist
    topics=("iot-temperature" "iot-window-events")
    
    for topic in "${topics[@]}"; do
        if ! docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -q "^$topic$"; then
            echo "Creating topic: $topic"
            docker-compose exec -T kafka kafka-topics \
                --bootstrap-server localhost:9092 \
                --create \
                --topic "$topic" \
                --partitions 3 \
                --replication-factor 1 2>/dev/null || true
        else
            echo -e "${GREEN}✓ Topic $topic exists${NC}"
        fi
    done
}

# Function to monitor Kafka topic messages
monitor_kafka_messages() {
    local topic=$1
    local max_messages=${2:-10}
    
    echo -e "\n${BLUE}Monitoring topic: $topic (showing last $max_messages messages)${NC}"
    
    timeout 10s docker-compose exec -T kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic "$topic" \
        --max-messages "$max_messages" \
        --from-beginning 2>/dev/null || echo "No messages or timeout reached"
}

# Function to check database tables
check_database() {
    echo -e "\n${BLUE}Checking database tables...${NC}"
    
    # Wait for PostgreSQL to be ready
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if docker-compose exec -T postgres pg_isready -U iot_user -d iot_data > /dev/null 2>&1; then
            break
        else
            echo -n "Waiting for PostgreSQL to be ready..."
            sleep 2
            attempt=$((attempt + 1))
        fi
    done
    
    if [ $attempt -gt $max_attempts ]; then
        echo -e "${RED}✗ PostgreSQL not ready${NC}"
        return 1
    fi
    
    # Check table record counts
    tables=("temperature_readings" "window_events" "temperature_aggregations_1min" "temperature_aggregations_5min")
    
    for table in "${tables[@]}"; do
        count=$(docker-compose exec -T postgres psql -U iot_user -d iot_data -t -c "SELECT COUNT(*) FROM $table;" 2>/dev/null | tr -d ' \n' || echo "0")
        if [ "$count" -gt 0 ]; then
            echo -e "${GREEN}✓ $table: $count records${NC}"
        else
            echo -e "${YELLOW}⚠ $table: $count records${NC}"
        fi
    done
    
    # Show latest records
    echo -e "\n${BLUE}Latest temperature readings:${NC}"
    docker-compose exec -T postgres psql -U iot_user -d iot_data -c \
        "SELECT device_id, temperature_celsius, event_timestamp, is_anomaly FROM temperature_readings ORDER BY event_timestamp DESC LIMIT 5;" 2>/dev/null || echo "No data available"
}

# Function to check Spark jobs
check_spark_jobs() {
    echo -e "\n${BLUE}Checking Spark streaming jobs...${NC}"
    
    # Check if Spark master is accessible
    if curl -f http://localhost:8080 > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Spark Master UI accessible at http://localhost:8080${NC}"
        
        # Try to get job information (simplified check)
        running_apps=$(curl -s http://localhost:8080/json/ 2>/dev/null | grep -o '"activeapps":\[[^]]*\]' | grep -o '\[.*\]' | grep -c '{' || echo "0")
        echo "Running Spark applications: $running_apps"
    else
        echo -e "${YELLOW}⚠ Spark Master UI not accessible${NC}"
    fi
    
    # Check Spark streaming logs
    echo -e "\n${BLUE}Recent Spark streaming logs:${NC}"
    docker-compose logs --tail=10 spark-streaming 2>/dev/null || echo "No streaming logs available"
}

# Function to check HDFS
check_hdfs() {
    echo -e "\n${BLUE}Checking HDFS...${NC}"
    
    if curl -f http://localhost:9870 > /dev/null 2>&1; then
        echo -e "${GREEN}✓ HDFS Namenode UI accessible at http://localhost:9870${NC}"
        
        # Try to list HDFS contents (simplified)
        echo "HDFS directory structure:"
        docker-compose exec -T namenode hdfs dfs -ls / 2>/dev/null || echo "Could not list HDFS contents"
    else
        echo -e "${YELLOW}⚠ HDFS Namenode UI not accessible${NC}"
    fi
}

# Function to display system status dashboard
show_status_dashboard() {
    clear
    echo -e "${BLUE}╔════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║        IoT System Status Dashboard     ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════╝${NC}"
    echo
    
    # Service status
    echo -e "${BLUE}Service Status:${NC}"
    services=("zookeeper" "kafka" "postgres" "spark-master" "grafana" "kafka-producer" "spark-streaming")
    
    for service in "${services[@]}"; do
        if docker-compose ps "$service" | grep -q "Up"; then
            echo -e "  ${GREEN}✓${NC} $service"
        else
            echo -e "  ${RED}✗${NC} $service"
        fi
    done
    
    echo
    
    # Data flow metrics
    echo -e "${BLUE}Data Flow Metrics:${NC}"
    
    # Kafka message counts (approximate)
    temp_messages=$(timeout 5s docker-compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list localhost:9092 --topic iot-temperature 2>/dev/null | awk -F: '{sum += $3} END {print sum}' || echo "N/A")
    window_messages=$(timeout 5s docker-compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list localhost:9092 --topic iot-window-events 2>/dev/null | awk -F: '{sum += $3} END {print sum}' || echo "N/A")
    
    echo "  Kafka Messages:"
    echo "    iot-temperature: $temp_messages"
    echo "    iot-window-events: $window_messages"
    
    # Database record counts
    temp_records=$(docker-compose exec -T postgres psql -U iot_user -d iot_data -t -c "SELECT COUNT(*) FROM temperature_readings;" 2>/dev/null | tr -d ' \n' || echo "N/A")
    window_records=$(docker-compose exec -T postgres psql -U iot_user -d iot_data -t -c "SELECT COUNT(*) FROM window_events;" 2>/dev/null | tr -d ' \n' || echo "N/A")
    
    echo "  Database Records:"
    echo "    Temperature readings: $temp_records"
    echo "    Window events: $window_records"
    
    echo
    echo -e "${BLUE}Access URLs:${NC}"
    echo "  Grafana: http://localhost:3000 (admin/admin)"
    echo "  Spark Master: http://localhost:8080"
    echo "  HDFS Namenode: http://localhost:9870"
    echo
    echo "Last updated: $(date)"
}

# Function to run continuous monitoring
monitor_continuously() {
    echo -e "${BLUE}Starting continuous monitoring... (Press Ctrl+C to stop)${NC}"
    echo "Refresh interval: 30 seconds"
    echo
    
    while true; do
        show_status_dashboard
        
        # Wait for 30 seconds or until interrupted
        for i in {30..1}; do
            echo -ne "\rNext refresh in: $i seconds "
            sleep 1
        done
        echo
    done
}

# Function to show help
show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Options:"
    echo "  --monitor, -m       Start continuous monitoring dashboard"
    echo "  --check-topics, -t  Check and create Kafka topics"
    echo "  --check-db, -d      Check database status"
    echo "  --check-spark, -s   Check Spark jobs status"
    echo "  --check-hdfs, -h    Check HDFS status"
    echo "  --status           Show current system status"
    echo "  --help             Show this help message"
    echo
    echo "Examples:"
    echo "  $0                 # Run complete system check"
    echo "  $0 --monitor       # Start continuous monitoring"
    echo "  $0 --check-topics  # Check Kafka topics only"
}

# Main function
main() {
    case "${1:-}" in
        --monitor|-m)
            monitor_continuously
            ;;
        --check-topics|-t)
            check_kafka_topics
            ;;
        --check-db|-d)
            check_database
            ;;
        --check-spark|-s)
            check_spark_jobs
            ;;
        --check-hdfs|-h)
            check_hdfs
            ;;
        --status)
            show_status_dashboard
            ;;
        --help)
            show_help
            ;;
        "")
            # Run complete system check
            echo "Running complete system check..."
            check_kafka_topics
            check_database
            check_spark_jobs
            check_hdfs
            
            echo -e "\n${GREEN}System check complete!${NC}"
            echo "Use --monitor flag for continuous monitoring"
            echo "Use --help for more options"
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            show_help
            exit 1
            ;;
    esac
}

# Trap Ctrl+C to exit gracefully
trap 'echo -e "\n${YELLOW}Monitoring stopped${NC}"; exit 0' INT

# Run main function with all arguments
main "$@"