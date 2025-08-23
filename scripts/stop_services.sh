#!/bin/bash
"""
Stop all IoT Data Processing System services
"""

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Stopping IoT Data Processing System ===${NC}"

# Parse command line arguments
REMOVE_VOLUMES=false
REMOVE_IMAGES=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --remove-volumes|-v)
            REMOVE_VOLUMES=true
            shift
            ;;
        --remove-images|-i)
            REMOVE_IMAGES=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo
            echo "Options:"
            echo "  --remove-volumes, -v    Remove all volumes (WARNING: This will delete all data)"
            echo "  --remove-images, -i     Remove Docker images after stopping"
            echo "  --help, -h             Show this help message"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Function to gracefully stop services
graceful_stop() {
    echo "Gracefully stopping services..."
    
    # Stop processing services first
    echo "Stopping data processing services..."
    docker-compose stop kafka-producer spark-streaming
    
    # Stop application services
    echo "Stopping application services..."
    docker-compose stop grafana
    
    # Stop Spark cluster
    echo "Stopping Spark cluster..."
    docker-compose stop spark-worker spark-master
    
    # Stop data services
    echo "Stopping data services..."
    docker-compose stop postgres redis
    
    # Stop Hadoop cluster
    echo "Stopping Hadoop cluster..."
    docker-compose stop datanode namenode
    
    # Stop Kafka and Zookeeper last
    echo "Stopping Kafka and Zookeeper..."
    docker-compose stop kafka zookeeper
}

# Function to force stop if graceful stop fails
force_stop() {
    echo -e "${YELLOW}Forcing stop of all services...${NC}"
    docker-compose down --timeout 30
}

# Main stop process
echo "Stopping all services..."

# Try graceful stop first
if ! graceful_stop; then
    echo -e "${YELLOW}Graceful stop failed, forcing stop...${NC}"
    force_stop
fi

# Remove containers
echo "Removing containers..."
if [ "$REMOVE_VOLUMES" = true ]; then
    echo -e "${YELLOW}WARNING: Removing all volumes - all data will be lost!${NC}"
    read -p "Are you sure? Type 'yes' to continue: " -r
    if [[ $REPLY == "yes" ]]; then
        docker-compose down -v --remove-orphans
        echo -e "${RED}All volumes removed!${NC}"
    else
        echo "Volume removal cancelled"
        docker-compose down --remove-orphans
    fi
else
    docker-compose down --remove-orphans
fi

# Remove images if requested
if [ "$REMOVE_IMAGES" = true ]; then
    echo "Removing Docker images..."
    
    # Get list of images used by this compose file
    images=$(docker-compose config | grep "image:" | awk '{print $2}' | sort | uniq)
    
    for image in $images; do
        echo "Removing image: $image"
        docker rmi "$image" 2>/dev/null || echo "Could not remove $image (may be in use)"
    done
    
    # Remove custom built images
    custom_images=$(docker images | grep -E "(kafka|spark)" | grep iot-data-processing | awk '{print $3}')
    for image_id in $custom_images; do
        echo "Removing custom image: $image_id"
        docker rmi "$image_id" 2>/dev/null || true
    done
fi

# Clean up dangling volumes and networks
echo "Cleaning up unused resources..."
docker system prune -f > /dev/null 2>&1 || true

# Show final status
echo
running_containers=$(docker-compose ps -q 2>/dev/null | wc -l)
if [ "$running_containers" -eq 0 ]; then
    echo -e "${GREEN}✓ All services stopped successfully!${NC}"
else
    echo -e "${YELLOW}⚠ Some containers may still be running${NC}"
    docker-compose ps
fi

echo
echo -e "${BLUE}Cleanup summary:${NC}"
echo "• Containers: Stopped and removed"
if [ "$REMOVE_VOLUMES" = true ]; then
    echo -e "• Volumes: ${RED}Removed (data lost)${NC}"
else
    echo -e "• Volumes: ${GREEN}Preserved${NC}"
fi
if [ "$REMOVE_IMAGES" = true ]; then
    echo "• Images: Removed"
else
    echo "• Images: Preserved"
fi

echo
echo "To restart the system, run: ./scripts/start_services.sh"

# Show disk space freed (if volumes were removed)
if [ "$REMOVE_VOLUMES" = true ]; then
    echo
    echo "Disk space has been freed by removing volumes."
    echo "Next startup will initialize fresh databases."
fi