.PHONY: test test-unit test-integration lint typecheck run pre-commit install

help:
	@echo "Доступные команды:"
	@echo "  make install      - Установить все зависимости"
	@echo "  make test         - Запустить тесты pytest"
	@echo "  make test-unit    - Запустить unit тесты"
	@echo "  make test-integration - Запустить integration тесты"
	@echo "  make run          - Запустить приложение"
	@echo "  make lint         - Запустить линтер ruff"
	@echo "  make typecheck    - Запустить проверку типов mypy"
	@echo "  make pre-commit   - Запустить все проверки (lint, typecheck, test)"

install:
	uv sync

test:
	uv run pytest -v

test-unit:
	uv run pytest -v -m "unit"

test-integration:
	uv run pytest -v -m "integration"

lint:
	uv run ruff check .

typecheck:
	uv run mypy .

run:
	uv run main.py 

pre-commit: lint typecheck test-unit
