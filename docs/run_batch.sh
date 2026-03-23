#!/bin/bash
# Usage: run_batch.sh <batch_name> <component1> <component2> ...
# Runs all components in parallel, waits for all to finish
set -uo pipefail

BATCH_NAME="$1"
shift
COMPONENTS=("$@")
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="/tmp/enrichment-logs"

mkdir -p "$LOG_DIR"

echo "====================================="
echo "BATCH: $BATCH_NAME"
echo "Components: ${COMPONENTS[*]}"
echo "Started: $(date)"
echo "====================================="

PIDS=()
for COMP in "${COMPONENTS[@]}"; do
    echo "[$(date)] Launching $COMP..."
    setsid bash "$SCRIPT_DIR/run_enrichment.sh" "$COMP" all \
        > "$LOG_DIR/batch_${COMP}.log" 2>&1 &
    PIDS+=($!)
    echo "[$(date)] $COMP launched (PID=${PIDS[-1]})"
    # Stagger launches by 10s to avoid initial token burst
    sleep 10
done

echo ""
echo "[$(date)] All ${#COMPONENTS[@]} components launched. Waiting..."
echo ""

# Wait for all and collect results
FAILED=()
SUCCEEDED=()
for i in "${!COMPONENTS[@]}"; do
    COMP="${COMPONENTS[$i]}"
    PID="${PIDS[$i]}"
    wait "$PID"
    RC=$?
    if [ $RC -eq 0 ] && [ -f "$LOG_DIR/${COMP}.done" ]; then
        SUCCEEDED+=("$COMP")
        echo "[$(date)] ✓ $COMP completed successfully"
    else
        FAILED+=("$COMP")
        echo "[$(date)] ✗ $COMP FAILED (rc=$RC)"
        # Show last 20 lines of log
        echo "--- Last 20 lines of $COMP log ---"
        tail -20 "$LOG_DIR/batch_${COMP}.log"
        echo "---"
    fi
done

echo ""
echo "====================================="
echo "BATCH $BATCH_NAME COMPLETE: $(date)"
echo "Succeeded: ${#SUCCEEDED[@]} — ${SUCCEEDED[*]:-none}"
echo "Failed: ${#FAILED[@]} — ${FAILED[*]:-none}"
echo "====================================="

# Write batch summary
cat > "$LOG_DIR/batch_${BATCH_NAME}_summary.txt" << EOF
Batch: $BATCH_NAME
Completed: $(date)
Succeeded (${#SUCCEEDED[@]}): ${SUCCEEDED[*]:-none}
Failed (${#FAILED[@]}): ${FAILED[*]:-none}
EOF

# Exit with failure if any component failed
[ ${#FAILED[@]} -eq 0 ]
