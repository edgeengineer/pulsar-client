#!/bin/bash
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DOCKER_DIR="$PROJECT_ROOT/docker"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Default values
COMPOSE_FILE="docker-compose.yml"
COMPOSE_ARGS=""
TIMEOUT=120

function usage() {
    echo "Usage: $0 [command] [options]"
    echo "Commands:"
    echo "  start    - Start Pulsar test environment"
    echo "  stop     - Stop Pulsar test environment"
    echo "  restart  - Restart Pulsar test environment"
    echo "  status   - Check environment status"
    echo "  logs     - Show container logs"
    echo "  clean    - Clean up all containers and volumes"
    echo ""
    echo "Options:"
    echo "  --cluster     Use full cluster setup"
    echo "  --auth        Use authentication setup"
    echo "  --ui          Include Pulsar Manager UI"
    echo "  --timeout N   Wait timeout in seconds (default: 120)"
}

function wait_for_pulsar() {
    local container=$1
    local timeout=$2
    local elapsed=0
    
    echo -e "${YELLOW}Waiting for Pulsar to be ready...${NC}"
    
    while [ $elapsed -lt $timeout ]; do
        if docker exec $container bin/pulsar-admin brokers healthcheck 2>/dev/null; then
            echo -e "${GREEN}Pulsar is ready!${NC}"
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
        echo -n "."
    done
    
    echo -e "${RED}Timeout waiting for Pulsar to be ready${NC}"
    return 1
}

function start_environment() {
    echo -e "${GREEN}Starting Pulsar test environment...${NC}"
    
    cd "$DOCKER_DIR"
    docker-compose -f "$COMPOSE_FILE" $COMPOSE_ARGS up -d
    
    # Wait for services to be ready
    if [[ "$COMPOSE_FILE" == *"auth"* ]]; then
        wait_for_pulsar "pulsar-auth" $TIMEOUT
    elif [[ "$COMPOSE_FILE" == *"cluster"* ]]; then
        wait_for_pulsar "broker" $TIMEOUT
    else
        wait_for_pulsar "pulsar-standalone" $TIMEOUT
    fi
    
    # Configure Toxiproxy if present
    if docker ps | grep -q toxiproxy; then
        echo -e "${YELLOW}Configuring Toxiproxy...${NC}"
        sleep 5
        curl -X POST http://localhost:8474/proxies \
          -H 'Content-Type: application/json' \
          -d '{
            "name": "pulsar",
            "listen": "0.0.0.0:16650",
            "upstream": "pulsar-standalone:6650",
            "enabled": true
          }' 2>/dev/null || true
    fi
    
    echo -e "${GREEN}Test environment is ready!${NC}"
    show_connection_info
}

function show_connection_info() {
    echo -e "\n${GREEN}Connection Information:${NC}"
    echo "  Pulsar Broker: pulsar://localhost:6650"
    echo "  Admin API: http://localhost:8080"
    
    if docker ps | grep -q toxiproxy; then
        echo "  Toxiproxy Pulsar: pulsar://localhost:16650"
        echo "  Toxiproxy API: http://localhost:8474"
    fi
    
    if docker ps | grep -q pulsar-manager; then
        echo "  Pulsar Manager UI: http://localhost:9527"
    fi
    
    if [[ "$COMPOSE_FILE" == *"auth"* ]]; then
        echo -e "\n${YELLOW}Authentication Tokens:${NC}"
        echo "  Admin Token:"
        docker exec pulsar-auth cat /pulsar/conf/admin.token 2>/dev/null || echo "    Not available"
        echo "  Client Token:"
        docker exec pulsar-auth cat /pulsar/conf/client.token 2>/dev/null || echo "    Not available"
    fi
}

# Parse command line arguments
COMMAND=$1
shift

while [[ $# -gt 0 ]]; do
    case $1 in
        --cluster)
            COMPOSE_FILE="docker-compose.cluster.yml"
            shift
            ;;
        --auth)
            COMPOSE_FILE="docker-compose.auth.yml"
            shift
            ;;
        --ui)
            COMPOSE_ARGS="$COMPOSE_ARGS --profile ui"
            shift
            ;;
        --timeout)
            TIMEOUT=$2
            shift 2
            ;;
        *)
            usage
            exit 1
            ;;
    esac
done

case $COMMAND in
    start)
        start_environment
        ;;
    stop)
        cd "$DOCKER_DIR"
        docker-compose -f "$COMPOSE_FILE" $COMPOSE_ARGS down
        ;;
    restart)
        cd "$DOCKER_DIR"
        docker-compose -f "$COMPOSE_FILE" $COMPOSE_ARGS restart
        ;;
    status)
        cd "$DOCKER_DIR"
        docker-compose -f "$COMPOSE_FILE" $COMPOSE_ARGS ps
        ;;
    logs)
        cd "$DOCKER_DIR"
        docker-compose -f "$COMPOSE_FILE" $COMPOSE_ARGS logs -f
        ;;
    clean)
        cd "$DOCKER_DIR"
        docker-compose -f "$COMPOSE_FILE" $COMPOSE_ARGS down -v
        ;;
    *)
        usage
        exit 1
        ;;
esac