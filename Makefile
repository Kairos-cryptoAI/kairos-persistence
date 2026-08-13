UV ?= uv

.PHONY: sync lint typecheck security test test-integration check migration-smoke lock

sync:
	$(UV) sync --locked --dev

lint:
	$(UV) run ruff check .

typecheck:
	$(UV) run mypy kairos_persistence

security:
	$(UV) run bandit -q -r kairos_persistence -x tests

test:
	$(UV) run pytest -q -m "not integration"

test-integration:
	$(UV) run pytest -q -m integration

migration-smoke:
	$(UV) run python scripts/migration_smoke.py

check: lint typecheck security test

lock:
	$(UV) lock
