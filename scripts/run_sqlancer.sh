#!/usr/bin/env bash
# scripts/run_sqlancer.sh — SQLancer differential fuzzing harness.
#
# Spins up the pg_trickle E2E Docker container and runs two testing layers:
#
#   1. Rust-based crash + equivalence oracle (SQLANCER-1 / SQLANCER-2)
#      `cargo nextest run --test e2e_sqlancer_tests -- --ignored --no-capture`
#
#   2. SQLancer Java tool (optional — requires JAVA_HOME or docker-based runner)
#      When SQLANCER_JAR is set, the SQLancer crash-test oracle is also run
#      using the PostgreSQLNoRECOracle / PostgreSQLPQSOracle.
#
# Environment variables
# ─────────────────────
#   SQLANCER_CASES       Number of Rust oracle test cases  (default: 200)
#   SQLANCER_SEED        Hex seed for Rust oracle           (default: random)
#   SQLANCER_JAR         Path to a pre-built sqlancer.jar   (optional)
#   SQLANCER_JAVA        Path to a Java 17+ binary          (default: java)
#   PGT_E2E_IMAGE        Docker image override              (default: pg_trickle_e2e:latest)
#   SKIP_RUST_ORACLE     Set to 1 to skip the Rust oracle
#   SKIP_JAVA_ORACLE     Set to 1 to skip the Java SQLancer oracle

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

E2E_IMAGE="${PGT_E2E_IMAGE:-pg_trickle_e2e:latest}"
SQLANCER_CASES="${SQLANCER_CASES:-200}"
JAVA_BIN="${SQLANCER_JAVA:-java}"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  pg_trickle SQLancer fuzzing harness"
echo "  image        : ${E2E_IMAGE}"
echo "  cases        : ${SQLANCER_CASES}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

cd "${REPO_ROOT}"

# ── Phase 1: Rust crash + equivalence oracle (SQLANCER-1 / SQLANCER-2) ──────
if [[ "${SKIP_RUST_ORACLE:-0}" != "1" ]]; then
    echo ""
    echo "▶ Phase 1 — Rust crash + equivalence oracle"
    echo "  (SQLANCER_CASES=${SQLANCER_CASES})"

    export SQLANCER_CASES
    if [[ -n "${SQLANCER_SEED:-}" ]]; then
        export SQLANCER_SEED
    fi

    cargo nextest run \
        --test e2e_sqlancer_tests \
        --run-ignored all \
        --no-capture \
        -E 'test(test_sqlancer_ci_combined)'

    echo "✔ Phase 1 complete"
fi

# ── Phase 2: Java SQLancer oracle (optional) ──────────────────────────────
if [[ "${SKIP_JAVA_ORACLE:-0}" != "1" ]] && [[ -n "${SQLANCER_JAR:-}" ]]; then
    if [[ ! -f "${SQLANCER_JAR}" ]]; then
        echo "WARNING: SQLANCER_JAR='${SQLANCER_JAR}' not found — skipping Java oracle"
    else
        echo ""
        echo "▶ Phase 2 — Java SQLancer oracle (jar=${SQLANCER_JAR})"

        # Start a dedicated Postgres container for SQLancer.
        SQLANCER_CONTAINER="pgt_sqlancer_$$"
        PG_PORT=15433

        cleanup_sqlancer_container() {
            docker rm -fv "${SQLANCER_CONTAINER}" >/dev/null 2>&1 || true
        }
        trap cleanup_sqlancer_container EXIT INT TERM

        echo "  Starting container ${SQLANCER_CONTAINER} on port ${PG_PORT}..."
        docker run -d \
            --name "${SQLANCER_CONTAINER}" \
            --label "com.pgtrickle.test=true" \
            --label "com.pgtrickle.suite=sqlancer" \
            -e POSTGRES_USER=postgres \
            -e POSTGRES_PASSWORD=postgres \
            -e POSTGRES_DB=testdb \
            -p "${PG_PORT}:5432" \
            "${E2E_IMAGE}"

        # Wait for Postgres to be ready.
        echo "  Waiting for Postgres to be ready..."
        for i in $(seq 1 30); do
            if docker exec "${SQLANCER_CONTAINER}" \
                pg_isready -U postgres -q 2>/dev/null; then
                break
            fi
            sleep 1
        done

        docker exec "${SQLANCER_CONTAINER}" \
            psql -U postgres -d testdb \
            -c "CREATE EXTENSION IF NOT EXISTS pg_trickle;"

        echo "  Running SQLancer crash-test oracle..."
        "${JAVA_BIN}" -jar "${SQLANCER_JAR}" \
            --dbms postgres \
            --host localhost \
            --port "${PG_PORT}" \
            --username postgres \
            --password postgres \
            --num-threads 1 \
            --num-tries 1000 \
            --oracle NOREC \
            postgres 2>&1 | tail -20

        echo "✔ Phase 2 complete"
    fi
elif [[ "${SKIP_JAVA_ORACLE:-0}" == "1" ]]; then
    echo ""
    echo "▶ Phase 2 — Java SQLancer oracle skipped (SKIP_JAVA_ORACLE=1)"
else
    echo ""
    echo "▶ Phase 2 — Java SQLancer oracle skipped (SQLANCER_JAR not set)"
    echo "  To enable: set SQLANCER_JAR=/path/to/sqlancer.jar"
    echo "  Download:  https://github.com/sqlancer/sqlancer/releases"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  SQLancer harness complete ✔"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
