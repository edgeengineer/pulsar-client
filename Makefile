.PHONY: help test test-unit test-integration docker-up docker-down clean

help:
	@echo "Available targets:"
	@echo "  test              - Run all tests"
	@echo "  test-unit         - Run unit tests only"
	@echo "  test-integration  - Run integration tests with Pulsar"
	@echo "  docker-up         - Start Pulsar test environment"
	@echo "  docker-down       - Stop Pulsar test environment"
	@echo "  docker-logs       - Show Pulsar container logs"
	@echo "  clean             - Clean build artifacts and containers"

test:
	./scripts/run-tests.sh

test-unit:
	./scripts/run-tests.sh --unit

test-integration:
	./scripts/run-tests.sh --integration --env standalone

docker-up:
	./scripts/test-env.sh start

docker-down:
	./scripts/test-env.sh stop

docker-logs:
	./scripts/test-env.sh logs

clean:
	./scripts/test-env.sh clean
	swift package clean
	rm -rf .build

# Development helpers
dev-setup:
	./scripts/setup-dev.sh

format:
	swift-format -i -r Sources/ Tests/

lint:
	swift-format lint -r Sources/ Tests/