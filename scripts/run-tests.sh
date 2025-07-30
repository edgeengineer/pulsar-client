#!/bin/bash
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default values
TEST_FILTER=""
TEST_TYPE="all"
VERBOSE=false
PARALLEL=true
COVERAGE_HTML=false
COVERAGE_JSON=false
COVERAGE_LCOV=false

function usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  --filter PATTERN     Run only tests matching pattern"
    echo "  --unit               Run only unit tests"
    echo "  --integration        Run only integration tests"
    echo "  --verbose            Enable verbose output"
    echo "  --no-parallel        Disable parallel test execution"
    echo "  --coverage-html      Enable HTML code coverage (unit tests only)"
    echo "  --coverage-json      Enable JSON code coverage (unit tests only)"
    echo "  --coverage-lcov      Enable LCOV code coverage (unit tests only)"
    echo "  --coverage-all       Enable all coverage formats (unit tests only)"
    echo "  --env TYPE           Start test environment (standalone|cluster|auth)"
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
        --coverage-html)
            COVERAGE_HTML=true
            TEST_TYPE="unit"  # Force unit tests for coverage
            shift
            ;;
        --coverage-json)
            COVERAGE_JSON=true
            TEST_TYPE="unit"  # Force unit tests for coverage
            shift
            ;;
        --coverage-lcov)
            COVERAGE_LCOV=true
            TEST_TYPE="unit"  # Force unit tests for coverage
            shift
            ;;
        --coverage-all)
            COVERAGE_HTML=true
            COVERAGE_JSON=true
            COVERAGE_LCOV=true
            TEST_TYPE="unit"  # Force unit tests for coverage
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

# Check if any coverage is enabled
COVERAGE_ENABLED=false
if [ "$COVERAGE_HTML" = true ] || [ "$COVERAGE_JSON" = true ] || [ "$COVERAGE_LCOV" = true ]; then
    COVERAGE_ENABLED=true
fi

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

if [ "$COVERAGE_ENABLED" = true ]; then
    TEST_CMD="$TEST_CMD --enable-code-coverage"
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

# Generate coverage reports if requested
if [ "$COVERAGE_ENABLED" = true ]; then
    echo "Generating coverage reports..."
    
    BINARY_PATH=".build/debug/PulsarClientPackageTests.xctest/Contents/MacOS/PulsarClientPackageTests"
    PROFILE_PATH=".build/debug/codecov/default.profdata"
    
    if [ "$COVERAGE_HTML" = true ]; then
        echo "  Generating HTML coverage report..."
        xcrun llvm-cov show \
            "$BINARY_PATH" \
            -instr-profile="$PROFILE_PATH" \
            Sources/ \
            --format=html \
            -o .build/coverage
        echo "  HTML coverage report: .build/coverage/index.html"
    fi
    
    if [ "$COVERAGE_JSON" = true ]; then
        echo "  Generating JSON coverage report..."
        xcrun llvm-cov export \
            "$BINARY_PATH" \
            -instr-profile="$PROFILE_PATH" \
            --include-directory=Sources/ \
            -format=json > coverage.json
        echo "  JSON coverage report: coverage.json"
    fi
    
    if [ "$COVERAGE_LCOV" = true ]; then
        echo "  Generating LCOV coverage report..."
        xcrun llvm-cov export \
            "$BINARY_PATH" \
            -instr-profile="$PROFILE_PATH" \
            --include-directory=Sources/ \
            -format=lcov > coverage.lcov
        echo "  LCOV coverage report: coverage.lcov"
    fi
    
    # Open HTML report if generated
    if [ "$COVERAGE_HTML" = true ]; then
        open .build/coverage/index.html
    fi
fi

# Stop test environment if we started it
if [ ! -z "$ENV_TYPE" ]; then
    "$SCRIPT_DIR/test-env.sh" stop
fi