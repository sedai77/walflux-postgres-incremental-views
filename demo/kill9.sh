#!/usr/bin/env bash
# WalFlux kill -9 demo: SIGKILL the daemon mid-stream, keep writing while it
# is dead, restart it, and prove the aggregates still match ground truth
# exactly. Safe to re-run; it tears down previous demo state first.
set -euo pipefail

# Always operate from the repo root, wherever the script was invoked from.
cd "$(dirname "$0")/.."
COMPOSE=(docker compose -f demo/docker-compose.yml)

if ! docker compose version >/dev/null 2>&1; then
    echo "error: this demo needs Docker with the compose v2 plugin" >&2
    exit 1
fi

step=0
say() {
    step=$((step + 1))
    echo
    echo "==> [$step] $*"
}

echo "WalFlux kill -9 demo: exactly-once aggregates across a hard daemon crash."

say "Cleaning up any previous demo state (docker compose down -v)"
"${COMPOSE[@]}" down -v --remove-orphans

say "Building the walflux image"
"${COMPOSE[@]}" build

say "Starting Postgres 16 (wal_level=logical) and seeding 50 orders"
"${COMPOSE[@]}" up -d --wait postgres

say "walflux setup: publication + replication slot + consistent snapshot backfill"
"${COMPOSE[@]}" run --rm walflux walflux setup -c /demo/config.yaml --force

say "Starting the walflux daemon and a ~30 writes/sec generator"
"${COMPOSE[@]}" up -d walflux generator

say "Letting the pipeline run for 20 seconds"
sleep 20

say "Status snapshot while healthy"
"${COMPOSE[@]}" run --rm walflux walflux status -c /demo/config.yaml

say "kill -9 the daemon: docker compose kill -s SIGKILL walflux (no graceful shutdown)"
"${COMPOSE[@]}" kill -s SIGKILL walflux

say "The generator keeps writing for 10 seconds while the daemon is dead (WAL piles up in the slot)"
sleep 10

say "Restarting the daemon: it must resume from its checkpoint and skip redelivered transactions"
"${COMPOSE[@]}" up -d walflux
sleep 3
"${COMPOSE[@]}" logs --tail=8 walflux || true

say "Stopping the generator so the source table holds still"
"${COMPOSE[@]}" stop generator

say "Verifying: ground-truth GROUP BY queries vs the walflux target tables"
"${COMPOSE[@]}" run --rm walflux python /demo/verify.py

say "Why this passed"
cat <<'EOF'
WalFlux applies each batch of source transactions AND its replication checkpoint
in one target-database transaction, so a crash can never persist the aggregates
without the checkpoint or vice versa. After the restart, the slot redelivers
everything since the last acknowledged position, and every transaction whose
commit LSN is at or below the stored checkpoint is skipped, so nothing is ever
double-counted.
EOF

echo
echo "The demo stack is still running for inspection; 'make down' removes it."
