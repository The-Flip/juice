.PHONY: help test lint format typecheck quality precommit serve backup pull-prod

help:
	@echo "juice Makefile commands:"
	@echo ""
	@echo "  make test       - Run test suite"
	@echo "  make lint       - Run ruff linter (auto-fix)"
	@echo "  make format     - Run ruff formatter"
	@echo "  make typecheck  - Run mypy type checking"
	@echo "  make quality    - Format, lint, and typecheck"
	@echo "  make precommit  - Run pre-commit hooks"
	@echo "  make serve      - Start the juice server"
	@echo "  make backup     - Pull a prod DB backup to data/backups/"
	@echo "  make pull-prod  - Refresh the local dev DB from production"
	@echo ""

test:
	uv run pytest

lint:
	uv run ruff check . --fix

format:
	uv run ruff format .

typecheck:
	uv run mypy juice

quality: format lint typecheck
	@echo "All quality checks passed!"

precommit:
	uv run pre-commit run --all-files

serve:
	uv run juice serve

backup:
	./scripts/backup-prod.sh

pull-prod:
	./scripts/sync-prod-to-dev.sh
