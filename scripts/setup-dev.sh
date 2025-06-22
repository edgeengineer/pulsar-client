#!/bin/bash
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Setting up PulsarClient development environment..."

# Check prerequisites
echo "Checking prerequisites..."

# Check Docker
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed. Please install Docker first."
    exit 1
fi

# Check Docker Compose
if ! command -v docker-compose &> /dev/null; then
    echo "Error: Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Check Swift
if ! command -v swift &> /dev/null; then
    echo "Error: Swift is not installed. Please install Swift 6.1 or later."
    exit 1
fi

# Create necessary directories
echo "Creating directory structure..."
mkdir -p "$PROJECT_ROOT/docker"
mkdir -p "$PROJECT_ROOT/scripts"
mkdir -p "$PROJECT_ROOT/Tests/PulsarClientIntegrationTests"

# Make scripts executable
chmod +x "$PROJECT_ROOT/scripts"/*.sh

# Install git hooks
echo "Installing git hooks..."
cat > "$PROJECT_ROOT/.git/hooks/pre-push" << 'EOF'
#!/bin/bash
# Run tests before push
echo "Running tests before push..."
swift test --filter PulsarClientTests
EOF
chmod +x "$PROJECT_ROOT/.git/hooks/pre-push"

# Create local environment file
cat > "$PROJECT_ROOT/.env.local" << EOF
# Local development environment variables
PULSAR_SERVICE_URL=pulsar://localhost:6650
PULSAR_ADMIN_URL=http://localhost:8080
EOF

echo "Development environment setup complete!"
echo ""
echo "Quick start commands:"
echo "  Start Pulsar:          ./scripts/test-env.sh start"
echo "  Run all tests:         ./scripts/run-tests.sh"
echo "  Run unit tests only:   ./scripts/run-tests.sh --unit"
echo "  Run integration tests: ./scripts/run-tests.sh --integration --env standalone"
echo ""
echo "For more options, see the README.md file."