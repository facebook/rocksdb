#!/bin/bash
# Usage: run_enrichment.sh <component> [step]
# Steps: enrich, review, fix, all (default: all)
# For split components, enrich runs sub-component CCs + stitcher automatically.
set -uo pipefail

COMPONENT="$1"
STEP="${2:-all}"
REPO="/home/xbw/workspace/ws36/rocksdb"
GENERATE="$REPO/docs/generate_prompt.py"
REVIEWS_DIR="$REPO/docs/reviews"
LOG_DIR="/tmp/enrichment-logs"
SPLITS_FILE="$REPO/docs/splits.json"

mkdir -p "$REVIEWS_DIR" "$LOG_DIR"

is_split_component() {
    python3 -c "
import json
splits = json.load(open('$SPLITS_FILE'))
exit(0 if '$COMPONENT' in splits else 1)
"
}

get_split_count() {
    python3 -c "
import json
splits = json.load(open('$SPLITS_FILE'))
print(len(splits.get('$COMPONENT', [])))
"
}

run_enrich_single() {
    echo "[$(date)] Starting enrichment for $COMPONENT (single CC)"
    local PROMPT_FILE="/tmp/enrich_prompt_${COMPONENT}.md"
    python3 "$GENERATE" "$COMPONENT" enrich > "$PROMPT_FILE"
    
    cd "$REPO"
    claude -p "$PROMPT_FILE" --allowedTools "Read,Write,Edit,Bash,Task" \
        > "$LOG_DIR/enrich_${COMPONENT}.log" 2>&1
    local rc=$?
    echo "[$(date)] Enrichment for $COMPONENT finished (rc=$rc)"
    return $rc
}

run_enrich_split() {
    local COUNT
    COUNT=$(get_split_count)
    echo "[$(date)] Starting split enrichment for $COMPONENT ($COUNT sub-components)"
    
    mkdir -p "$REPO/docs/components/$COMPONENT"
    
    # Launch all sub-component CCs in parallel
    local PIDS=()
    for i in $(seq 0 $((COUNT - 1))); do
        local SUB_NAME
        SUB_NAME=$(python3 -c "
import json
splits = json.load(open('$SPLITS_FILE'))
print(splits['$COMPONENT'][$i]['name'])
")
        local PROMPT_FILE="/tmp/enrich_sub_${COMPONENT}_${i}.md"
        python3 "$GENERATE" "$COMPONENT" enrich_sub "$i" > "$PROMPT_FILE"
        
        echo "[$(date)] Launching sub-component $i: $SUB_NAME"
        cd "$REPO"
        claude -p "$PROMPT_FILE" --allowedTools "Read,Write,Edit,Bash,Task" \
            > "$LOG_DIR/enrich_${COMPONENT}_sub${i}.log" 2>&1 &
        PIDS+=($!)
        sleep 5  # stagger to avoid burst
    done
    
    # Wait for all sub-components
    local FAILED=0
    for i in "${!PIDS[@]}"; do
        wait "${PIDS[$i]}"
        local rc=$?
        if [ $rc -ne 0 ]; then
            echo "[$(date)] WARNING: sub-component $i failed (rc=$rc)"
            FAILED=$((FAILED + 1))
        else
            echo "[$(date)] Sub-component $i completed"
        fi
    done
    
    if [ $FAILED -gt 0 ]; then
        echo "[$(date)] $FAILED sub-component(s) failed"
    fi
    
    # Run index stitcher
    echo "[$(date)] Running index stitcher for $COMPONENT"
    local STITCH_PROMPT="/tmp/stitch_prompt_${COMPONENT}.md"
    python3 "$GENERATE" "$COMPONENT" stitch > "$STITCH_PROMPT"
    
    cd "$REPO"
    claude -p "$STITCH_PROMPT" --allowedTools "Read,Write,Edit,Bash" \
        > "$LOG_DIR/stitch_${COMPONENT}.log" 2>&1
    local rc=$?
    echo "[$(date)] Index stitcher for $COMPONENT finished (rc=$rc)"
    return $rc
}

run_enrich() {
    if is_split_component; then
        run_enrich_split
    else
        run_enrich_single
    fi
    
    # Verify output
    if [ -d "$REPO/docs/components/$COMPONENT" ] && [ -f "$REPO/docs/components/$COMPONENT/index.md" ]; then
        local CHAPTERS
        CHAPTERS=$(ls "$REPO/docs/components/$COMPONENT"/*.md 2>/dev/null | grep -v index | wc -l)
        echo "[$(date)] SUCCESS: $COMPONENT/ created with index.md + $CHAPTERS chapters"
    else
        echo "[$(date)] WARNING: $COMPONENT/index.md not found"
    fi
}

run_review_cc() {
    echo "[$(date)] Starting CC review for $COMPONENT"
    local PROMPT_FILE="/tmp/review_cc_prompt_${COMPONENT}.md"
    python3 "$GENERATE" "$COMPONENT" review_cc > "$PROMPT_FILE"
    
    cd "$REPO"
    claude -p "$PROMPT_FILE" --allowedTools "Read,Write,Edit,Bash" \
        > "$LOG_DIR/review_cc_${COMPONENT}.log" 2>&1
    local rc=$?
    echo "[$(date)] CC review for $COMPONENT finished (rc=$rc)"
    return $rc
}

run_review_codex() {
    echo "[$(date)] Starting Codex review for $COMPONENT"
    local PROMPT_FILE="/tmp/review_codex_prompt_${COMPONENT}.md"
    python3 "$GENERATE" "$COMPONENT" review_codex > "$PROMPT_FILE"
    
    cd "$REPO"
    codex exec "$PROMPT_FILE" \
        > "$LOG_DIR/review_codex_${COMPONENT}.log" 2>&1
    local rc=$?
    echo "[$(date)] Codex review for $COMPONENT finished (rc=$rc)"
    return $rc
}

run_fix() {
    echo "[$(date)] Starting fix+debate for $COMPONENT"
    local PROMPT_FILE="/tmp/fix_prompt_${COMPONENT}.md"
    python3 "$GENERATE" "$COMPONENT" fix > "$PROMPT_FILE"
    
    cd "$REPO"
    claude -p "$PROMPT_FILE" --allowedTools "Read,Write,Edit,Bash" \
        > "$LOG_DIR/fix_${COMPONENT}.log" 2>&1
    local rc=$?
    echo "[$(date)] Fix for $COMPONENT finished (rc=$rc)"
    return $rc
}

run_all() {
    local START=$(date +%s)
    
    # Step 1: Enrichment
    run_enrich
    if [ $? -ne 0 ]; then
        echo "[$(date)] FAILED at enrichment for $COMPONENT"
        return 1
    fi
    
    # Step 2: Parallel reviews (CC + Codex)
    echo "[$(date)] Starting parallel reviews for $COMPONENT"
    run_review_cc &
    local CC_PID=$!
    run_review_codex &
    local CODEX_PID=$!
    
    wait $CC_PID
    local CC_RC=$?
    wait $CODEX_PID
    local CODEX_RC=$?
    
    if [ $CC_RC -ne 0 ]; then
        echo "[$(date)] WARNING: CC review failed (rc=$CC_RC)"
    fi
    if [ $CODEX_RC -ne 0 ]; then
        echo "[$(date)] WARNING: Codex review failed (rc=$CODEX_RC)"
    fi
    
    # Step 3: Fix + debate
    if [ -f "$REVIEWS_DIR/${COMPONENT}_review_cc.md" ] || [ -f "$REVIEWS_DIR/${COMPONENT}_review_codex.md" ]; then
        run_fix
    else
        echo "[$(date)] SKIPPING fix — no review files found"
    fi
    
    local END=$(date +%s)
    local ELAPSED=$(( END - START ))
    echo "[$(date)] COMPLETE: $COMPONENT pipeline done in ${ELAPSED}s"
    echo "done" > "$LOG_DIR/${COMPONENT}.done"
}

case "$STEP" in
    enrich) run_enrich ;;
    review) run_review_cc & run_review_codex & wait ;;
    fix) run_fix ;;
    all) run_all ;;
    *) echo "Unknown step: $STEP"; exit 1 ;;
esac
