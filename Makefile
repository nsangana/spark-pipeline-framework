.PHONY: help install install-dev test test-unit test-integration lint format typecheck check clean run-example

help:
	@echo "Spark Pipeline Framework - Available Commands"
	@echo ""
	@echo "Setup:"
	@echo "  make install          - Install production dependencies"
	@echo "  make install-dev      - Install development dependencies"
	@echo ""
	@echo "Testing:"
	@echo "  make test             - Run all tests"
	@echo "  make test-unit        - Run unit tests only"
	@echo "  make test-integration - Run integration tests only"
	@echo "  make test-coverage    - Run tests with coverage report"
	@echo ""
	@echo "Code Quality:"
	@echo "  make lint             - Run linter (ruff)"
	@echo "  make format           - Format code (black)"
	@echo "  make typecheck        - Run type checker (mypy)"
	@echo "  make check            - Run all quality checks"
	@echo ""
	@echo "Examples:"
	@echo "  make generate-data    - Generate sample data for examples"
	@echo "  make run-example      - Run user analytics example"
	@echo ""
	@echo "Utilities:"
	@echo "  make clean            - Remove generated files and caches"

install:
	pip install -r requirements.txt
	pip install -e .

install-dev:
	pip install -r requirements-dev.txt
	pip install -e .

test:
	pytest tests/ -v

test-unit:
	pytest tests/unit/ -v

test-integration:
	pytest tests/integration/ -v

test-coverage:
	pytest tests/ --cov=spark_pipeline --cov-report=html --cov-report=term

lint:
	ruff check spark_pipeline/ tests/ transformations/

format:
	black spark_pipeline/ tests/ transformations/ scripts/

typecheck:
	mypy spark_pipeline/

check: lint typecheck test

generate-data:
	python examples/user_analytics/scripts/generate_sample_data.py

run-example:
	@echo "Setting up environment variables..."
	@export DATA_PATH=$$(pwd)/examples/user_analytics/data && \
	export OUTPUT_PATH=$$(pwd)/examples/user_analytics/output && \
	echo "DATA_PATH=$$DATA_PATH" && \
	echo "OUTPUT_PATH=$$OUTPUT_PATH" && \
	python scripts/run_pipeline.py configs/examples/user_analytics.yaml

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name ".coverage" -delete 2>/dev/null || true
	find . -type d -name "spark-warehouse" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "derby.log" -delete 2>/dev/null || true
	rm -rf metastore_db/ 2>/dev/null || true
	@echo "Cleaned up generated files and caches"
