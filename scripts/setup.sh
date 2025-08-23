#!/bin/bash
"""
Initial setup script for IoT Data Processing System
"""

set -e

echo "=== IoT Data Processing System Setup ==="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if Docker and Docker Compose are installed
check_docker() {
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}Docker is not installed. Please install Docker first.${NC}"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        echo -e "${RED}Docker Compose is not installed. Please install Docker Compose first.${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✓ Docker and Docker Compose are installed${NC}"
}

# Check system resources
check_resources() {
    echo "Checking system resources..."
    
    # Check available RAM (in GB)
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        TOTAL_RAM=$(free -g | awk '/^Mem:/{print $2}')
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        TOTAL_RAM=$(( $(sysctl -n hw.memsize) / 1024 / 1024 / 1024 ))
    else
        TOTAL_RAM=8  # Assume sufficient RAM for Windows/other
    fi
    
    if [ "$TOTAL_RAM" -lt 8 ]; then
        echo -e "${YELLOW}Warning: Recommended minimum RAM is 8GB. Available: ${TOTAL_RAM}GB${NC}"
        echo -e "${YELLOW}The system may run slowly or encounter memory issues.${NC}"
        read -p "Continue anyway? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    else
        echo -e "${GREEN}✓ Sufficient RAM available: ${TOTAL_RAM}GB${NC}"
    fi
}

# Check if data files exist
check_data_files() {
    echo "Checking data files..."
    
    if [ ! -f "data/temperature.csv" ]; then
        echo -e "${YELLOW}Warning: data/temperature.csv not found${NC}"
        echo "Please ensure your temperature data is in the data/ directory"
    else
        echo -e "${GREEN}✓ Temperature data file found${NC}"
    fi
    
    if [ ! -f "data/window_opened_closed.csv" ]; then
        echo -e "${YELLOW}Warning: data/window_opened_closed.csv not found${NC}"
        echo "Please ensure your window data is in the data/ directory"
    else
        echo -e "${GREEN}✓ Window data file found${NC}"
    fi
}

# Create necessary directories
create_directories() {
    echo "Creating necessary directories..."
    
    directories=(
        "logs"
        "spark/logs"
        "grafana/data"
        "postgres/data"
        "kafka/logs"
    )
    
    for dir in "${directories[@]}"; do
        if [ ! -d "$dir" ]; then
            mkdir -p "$dir"
            echo -e "${GREEN}✓ Created directory: $dir${NC}"
        fi
    done
}

# Set permissions
set_permissions() {
    echo "Setting permissions..."
    
    # Make scripts executable
    find scripts/ -name "*.sh" -exec chmod +x {} \;
    chmod +x spark/jobs/submit_jobs.sh
    
    # Set directory permissions
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # For Linux, set appropriate permissions for Docker volumes
        sudo chown -R $USER:$USER logs/ spark/logs/ 2>/dev/null || true
    fi
    
    echo -e "${GREEN}✓ Permissions set${NC}"
}

# Pull required Docker images
pull_images() {
    echo "Pulling required Docker images... (this may take a while)"
    
    # Pull images in parallel to speed up the process
    docker-compose pull &
    PULL_PID=$!
    
    # Show progress
    while kill -0 $PULL_PID 2>/dev/null; do
        echo -n "."
        sleep 2
    done
    
    wait $PULL_PID
    
    if [ $? -eq 0 ]; then
        echo -e "\n${GREEN}✓ Docker images pulled successfully${NC}"
    else
        echo -e "\n${RED}✗ Failed to pull Docker images${NC}"
        exit 1
    fi
}

# Create .env file if it doesn't exist
create_env_file() {
    if [ ! -f ".env" ]; then
        echo "Creating .env file..."
        cat > .env << EOF
# PostgreSQL Configuration
POSTGRES_DB=iot_data
POSTGRES_USER=iot_user
POSTGRES_PASSWORD=iot_password

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:29092

# Spark Configuration
SPARK_MASTER_URL=spark://spark-master:7077

# Grafana Configuration
GF_SECURITY_ADMIN_USER=admin
GF_SECURITY_ADMIN_PASSWORD=admin

# HDFS Configuration
HDFS_NAMENODE=hdfs://namenode:9000
EOF
        echo -e "${GREEN}✓ .env file created${NC}"
    else
        echo -e "${GREEN}✓ .env file already exists${NC}"
    fi
}

# Validate docker-compose.yml
validate_compose() {
    echo "Validating docker-compose.yml..."
    
    if docker-compose config > /dev/null 2>&1; then
        echo -e "${GREEN}✓ docker-compose.yml is valid${NC}"
    else
        echo -e "${RED}✗ docker-compose.yml has errors${NC}"
        echo "Running docker-compose config to show errors:"
        docker-compose config
        exit 1
    fi
}

# Initialize Git repository
init_git() {
    if [ ! -d ".git" ]; then
        echo "Initializing Git repository..."
        git init
        git add .
        git commit -m "Initial commit: IoT Data Processing System Phase 2"
        echo -e "${GREEN}✓ Git repository initialized${NC}"
    else
        echo -e "${GREEN}✓ Git repository already exists${NC}"
    fi
}

# Main setup function
main() {
    echo "Starting setup process..."
    echo
    
    check_docker
    check_resources
    check_data_files
    create_directories
    set_permissions
    create_env_file
    validate_compose
    
    # Ask user if they want to pull images now
    echo
    read -p "Pull Docker images now? This will take several minutes. (Y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Nn]$ ]]; then
        echo -e "${YELLOW}Skipping image pull. You can run 'docker-compose pull' later.${NC}"
    else
        pull_images
    fi
    
    # Ask about Git initialization
    echo
    read -p "Initialize Git repository? (Y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Nn]$ ]]; then
        init_git
    fi
    
    echo
    echo -e "${GREEN}=== Setup Complete! ===${NC}"
    echo
    echo "Next steps:"
    echo "1. Ensure your data files are in the data/ directory"
    echo "2. Run './scripts/start_services.sh' to start all services"
    echo "3. Wait for services to be ready (check with 'docker-compose ps')"
    echo "4. Run './scripts/data_ingestion.sh' to start data ingestion"
    echo "5. Access Grafana dashboard at http://localhost:3000 (admin/admin)"
    echo
    echo "For more information, see README.md"
}

# Run main function
main "$@"