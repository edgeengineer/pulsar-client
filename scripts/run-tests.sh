#!/bin/bash
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default values
TEST_FILTER=""
TEST_TYPE="all"
VERBOSE=false
PARALLEL=true

function usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  --filter PATTERN   Run only tests matching pattern"
    echo "  --unit             Run only unit tests"
    echo "  --integration      Run only integration tests"
    echo "  --verbose          Enable verbose output"
    echo "  --no-parallel      Disable parallel test execution"
    echo "  --env TYPE         Start test environment (standalone|cluster|auth)"
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --filter)
            TEST_FILTER="$2"
            shift 2
            ;;
        --unit)
            TEST_TYPE="unit"
            shift
            ;;
        --integration)
            TEST_TYPE="integration"
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --no-parallel)
            PARALLEL=false
            shift
            ;;
        --env)
            ENV_TYPE="$2"
            shift 2
            ;;
        *)
            usage
            exit 1
            ;;
    esac
done

# Start test environment if requested
if [ ! -z "$ENV_TYPE" ]; then
    case $ENV_TYPE in
        standalone)
            "$SCRIPT_DIR/test-env.sh" start
            ;;
        cluster)
            "$SCRIPT_DIR/test-env.sh" start --cluster
            ;;
        auth)
            "$SCRIPT_DIR/test-env.sh" start --auth
            ;;
    esac
fi

# Build test command
TEST_CMD="swift test"

if [ "$VERBOSE" = true ]; then
    TEST_CMD="$TEST_CMD --verbose"
fi

if [ "$PARALLEL" = false ]; then
    TEST_CMD="$TEST_CMD --parallel-workers 1"
fi

if [ ! -z "$TEST_FILTER" ]; then
    TEST_CMD="$TEST_CMD --filter $TEST_FILTER"
fi

# Run tests based on type
cd "$PROJECT_ROOT"

case $TEST_TYPE in
    unit)
        echo "Running unit tests..."
        TEST_CMD="$TEST_CMD --filter PulsarClientTests"
        ;;
    integration)
        echo "Running integration tests..."
        TEST_CMD="$TEST_CMD --filter IntegrationTests"
        ;;
    all)
        echo "Running all tests..."
        ;;
esac

# Execute tests
eval $TEST_CMD

# Stop test environment if we started it
if [ ! -z "$ENV_TYPE" ]; then
    "$SCRIPT_DIR/test-env.sh" stop
fi