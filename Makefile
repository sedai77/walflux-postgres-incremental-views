# WalFlux developer and demo entry points. `make kill9` is the headline act.

COMPOSE := docker compose -f demo/docker-compose.yml

.PHONY: demo kill9 verify down test lint

# Build the demo stack, run setup, start the daemon + generator, follow daemon logs.
demo:
	@docker compose version >/dev/null 2>&1 || \
		{ echo "error: the demo needs Docker with the compose v2 plugin" >&2; exit 1; }
	$(COMPOSE) build
	$(COMPOSE) up -d --wait postgres
	$(COMPOSE) run --rm walflux walflux setup -c /demo/config.yaml --force
	$(COMPOSE) up -d walflux generator
	@$(COMPOSE) logs -f walflux || true

# The crash-safety proof: SIGKILL the daemon mid-stream, then verify exact aggregates.
kill9:
	bash demo/kill9.sh

# Compare ground-truth GROUP BY queries against the walflux target tables.
verify:
	$(COMPOSE) stop generator
	$(COMPOSE) run --rm walflux python /demo/verify.py

# Tear down the demo stack, including volumes.
down:
	$(COMPOSE) down -v --remove-orphans

# Unit tests (the integration suite needs Postgres; see CONTRIBUTING.md).
test:
	uv run --extra dev pytest -m "not integration"

lint:
	uv run --extra dev ruff check .
