.PHONY: help test test-unit test-integration docker-up docker-down clean dev-setup format lint coverage coverage-json coverage-lcov coverage-all

help: ## Show this help message
	@echo "Available targets:"
	@echo ""
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "Usage: make <target>"

test: ## Run all tests
	./scripts/run-tests.sh

test-unit: ## Run unit tests only
	./scripts/run-tests.sh --unit

test-integration: ## Run integration tests with Pulsar
	./scripts/run-tests.sh --integration --env standalone

docker-up: ## Start Pulsar test environment
	./scripts/test-env.sh start

docker-down: ## Stop Pulsar test environment
	./scripts/test-env.sh stop

docker-logs: ## Show Pulsar container logs
	./scripts/test-env.sh logs

clean: ## Clean build artifacts and containers
	./scripts/test-env.sh clean
	swift package clean
	rm -rf .build
	rm -rf .build/coverage
	rm -f coverage.json coverage.lcov

dev-setup: ## Set up development environment
	./scripts/setup-dev.sh

format: ## Format Swift code
	swift-format -i -r Sources/ Tests/

lint: ## Lint Swift code
	swift-format lint -r Sources/ Tests/

coverage: ## Generate and open HTML test coverage report (unit tests only)
	./scripts/run-tests.sh --coverage-html

coverage-json: ## Generate JSON test coverage report (unit tests only)
	./scripts/run-tests.sh --coverage-json

coverage-lcov: ## Generate LCOV test coverage report (unit tests only)
	./scripts/run-tests.sh --coverage-lcov

coverage-all: ## Generate all test coverage formats (unit tests only)
	./scripts/run-tests.sh --coverage-all
