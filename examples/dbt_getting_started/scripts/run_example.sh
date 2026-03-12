#!/usr/bin/env bash
# =============================================================================
# Run the pg_trickle Getting Started dbt example.
#
# Starts a pg_trickle E2E Docker container, runs the full dbt flow, waits
# for stream tables to populate, runs dbt tests, and cleans up.
#
# Usage:
#   ./examples/dbt_getting_started/scripts/run_example.sh
#   ./examples/dbt_getting_started/scripts/run_example.sh --skip-build
#   ./examples/dbt_getting_started/scripts/run_example.sh --keep-container
#
# Environment variables:
#   DBT_VERSION     dbt-core version to install (default: 1.10)
#   PGPORT          PostgreSQL port (default: 15433, avoids conflict with integration_tests on 15432)
#   CONTAINER_NAME  Docker container name (default: pgtrickle-getting-started)
#   SKIP_BUILD      Set to "1" to skip Docker image rebuild
#   KEEP_CONTAINER  Set to "1" to keep the container after the run
# =============================================================================
set -euo pipefail

# ── Configuration ───────────────────────────────────────────────────────────
DBT_VERSION="${DBT_VERSION:-1.10}"
PGPORT="${PGPORT:-15433}"
CONTAINER_NAME="${CONTAINER_NAME:-pgtrickle-getting-started}"
SKIP_BUILD="${SKIP_BUILD:-0}"
KEEP_CONTAINER="${KEEP_CONTAINER:-0}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
EXAMPLE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_ROOT="$(cd "$EXAMPLE_DIR/../.." && pwd)"

# Ensure psql is on PATH (Homebrew postgresql kegs may not be linked)
for _pg_dir in \
    /opt/homebrew/opt/postgresql@18/bin \
    /opt/homebrew/opt/postgresql@17/bin \
    /opt/homebrew/opt/postgresql@16/bin \
    /usr/local/opt/postgresql@18/bin \
    /usr/local/opt/postgresql@17/bin; do
  if [[ -x "$_pg_dir/psql" ]]; then
    export PATH="$_pg_dir:$PATH"
    break
  fi
done

# ── Parse flags ─────────────────────────────────────────────────────────────
for arg in "$@"; do
  case "$arg" in
    --skip-build) SKIP_BUILD=1 ;;
    --keep-container) KEEP_CONTAINER=1 ;;
    --help|-h)
      head -20 "$0" | tail -16
      exit 0
      ;;
    *)
      echo "Unknown argument: $arg" >&2
      exit 1
      ;;
  esac
done

# ── Cleanup trap ────────────────────────────────────────────────────────────
cleanup() {
  if [ "$KEEP_CONTAINER" = "0" ]; then
    echo ""
    echo "Cleaning up container ${CONTAINER_NAME}..."
    docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
  else
    echo ""
    echo "Container ${CONTAINER_NAME} kept running on port ${PGPORT}."
    echo "Connect: PGPORT=${PGPORT} psql -h localhost -U postgres"
    echo "Remove:  docker rm -f ${CONTAINER_NAME}"
  fi
}
trap cleanup EXIT

# ── Build Docker image (optional) ──────────────────────────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  pg_trickle Getting Started dbt example"
echo "  dbt version: ${DBT_VERSION}"
echo "  Port: ${PGPORT}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

if [ "$SKIP_BUILD" = "0" ]; then
  echo "Building E2E Docker image..."
  "$PROJECT_ROOT/tests/build_e2e_image.sh"
  echo ""
else
  echo "Skipping Docker image build (--skip-build)"
  if ! docker image inspect pg_trickle_e2e:latest &>/dev/null; then
    echo "ERROR: pg_trickle_e2e:latest not found. Run without --skip-build first." >&2
    exit 1
  fi
  echo ""
fi

# ── Start container ─────────────────────────────────────────────────────────
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

echo "Starting PostgreSQL with pg_trickle on port ${PGPORT}..."
docker run -d \
  --name "$CONTAINER_NAME" \
  -e POSTGRES_PASSWORD=postgres \
  -p "${PGPORT}:5432" \
  pg_trickle_e2e:latest

echo "Waiting for PostgreSQL to accept connections..."
for i in $(seq 1 30); do
  if docker exec "$CONTAINER_NAME" pg_isready -U postgres -q 2>/dev/null; then
    echo "PostgreSQL is ready (attempt $i)"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "ERROR: PostgreSQL not ready after 30s" >&2
    docker logs "$CONTAINER_NAME" | tail -20
    exit 1
  fi
  sleep 1
done

echo "Creating pg_trickle extension..."
docker exec "$CONTAINER_NAME" \
  psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS pg_trickle;"

# ── Set up dbt Python environment ────────────────────────────────────────────
# dbt-core 1.10 requires Python <=3.13 (mashumaro dependency incompatible with 3.14).
# Use a dedicated .venv-dbt virtual environment so we don't pollute the main venv.
DBT_VENV="$PROJECT_ROOT/.venv-dbt"
if [[ ! -f "$DBT_VENV/bin/activate" ]]; then
  echo "Creating dbt venv at $DBT_VENV using Python 3.13..."
  PYTHON_DBT="$(command -v python3.13 || command -v python3.12 || command -v python3)"
  "$PYTHON_DBT" -m venv "$DBT_VENV"
fi
# shellcheck source=/dev/null
source "$DBT_VENV/bin/activate"
if ! command -v dbt &>/dev/null; then
  echo ""
  echo "Installing dbt-core~=${DBT_VERSION}.0 and dbt-postgres~=${DBT_VERSION}.0..."
  pip install --quiet "dbt-core~=${DBT_VERSION}.0" "dbt-postgres~=${DBT_VERSION}.0"
fi
echo ""
echo "dbt version:"
dbt --version
echo ""

# ── Run example ─────────────────────────────────────────────────────────────
export PGHOST=localhost
export PGPORT
export PGUSER=postgres
export PGPASSWORD=postgres
export PGDATABASE=postgres

cd "$EXAMPLE_DIR"

echo "════════════════════════════════════════════════════════════════"
echo "  Running dbt Getting Started example"
echo "════════════════════════════════════════════════════════════════"

echo ""
echo "── dbt deps ──"
dbt deps

echo ""
echo "── dbt seed ──"
dbt seed

echo ""
echo "── dbt run (initial create) ──"
dbt run

echo ""
echo "── Waiting for stream tables to be populated ──"
for ST in public.department_tree public.department_stats public.department_report; do
  bash "$SCRIPT_DIR/wait_for_populated.sh" "$ST" 30
done

echo ""
echo "── dbt test ──"
dbt test

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  All checks passed!"
echo "════════════════════════════════════════════════════════════════"
