#!/usr/bin/env bash
# eval/reproduce_eval.sh
#
# One-command reproduction of the FDB-v3 (Full-Duplex-Bench v3) evaluation
# for ChronoCortex-Saga (CCS-Agent).
#
# Usage:
#   ./eval/reproduce_eval.sh [--gpu 0] [--results-dir ./results]
#
# Requirements: single 48GB GPU (CUDA 12.x/13.x) OR declared hosted APIs
# (set DEEPGRAM_API_KEY / CARTESIA_API_KEY / LLM provider key as env vars —
# see .env.example). No fine-tuning occurs; this script only runs inference
# and scoring.

set -euo pipefail

GPU_ID="${1:-0}"
RESULTS_DIR="${RESULTS_DIR:-./results/fdb_v3_$(date +%Y%m%d_%H%M%S)}"
FDBENCH_DIR="${FDBENCH_DIR:-./benchmarks/fdb-v3}"

echo "=== ChronoCortex-Saga :: FDB-v3 Reproduction ==="
echo "GPU: ${GPU_ID}   Results: ${RESULTS_DIR}"

mkdir -p "${RESULTS_DIR}"

echo "[1/6] Checking environment..."
python3 --version
if command -v nvidia-smi >/dev/null 2>&1; then
    nvidia-smi --query-gpu=name,memory.total,memory.used --format=csv -i "${GPU_ID}"
else
    echo "  no local GPU detected — assuming hosted-API mode"
fi

echo "[2/6] Installing pinned dependencies..."
python3 -m pip install -q --upgrade pip
python3 -m pip install -q -r requirements.txt

echo "[3/6] Fetching FDB-v3 benchmark data (arXiv:2604.04847) if not cached..."
if [ ! -d "${FDBENCH_DIR}" ]; then
    echo "  ERROR: ${FDBENCH_DIR} not found. Place the FDB-v3 dataset there"
    echo "  per the benchmark's official distribution instructions, then re-run."
    exit 1
fi

echo "[4/6] Launching CCS-Agent worker (headless eval mode)..."
CUDA_VISIBLE_DEVICES="${GPU_ID}" python3 -m agent \
    --eval-mode \
    --fdb-dir "${FDBENCH_DIR}" \
    --results-dir "${RESULTS_DIR}" \
    --domains travel_identity,finance_billing,housing_location,ecommerce_support \
    --chain-depths 1,2,3 \
    &
AGENT_PID=$!
trap 'kill ${AGENT_PID} 2>/dev/null || true' EXIT

echo "[5/6] Running FDB-v3 scenario harness against the agent (79 scenarios, 12 speakers)..."
python3 -m fdbench.runner \
    --benchmark-dir "${FDBENCH_DIR}" \
    --target-endpoint "ws://localhost:7880" \
    --output "${RESULTS_DIR}/raw_transcripts.jsonl"

echo "[6/6] Scoring: latency, self-correction pass rate, interruption rate, Pass@1..."
python3 -m fdbench.score \
    --input "${RESULTS_DIR}/raw_transcripts.jsonl" \
    --output "${RESULTS_DIR}/scorecard.json" \
    --metrics latency,self_correction_pass_rate,interruption_rate,pass_at_1

echo ""
echo "=== Results written to ${RESULTS_DIR}/scorecard.json ==="
python3 - "${RESULTS_DIR}/scorecard.json" <<'PYEOF'
import json, sys
with open(sys.argv[1]) as f:
    scores = json.load(f)
print(f"  Mean latency:              {scores.get('mean_latency_s', 'n/a')} s")
print(f"  Self-correction pass rate: {scores.get('self_correction_pass_rate', 'n/a')}")
print(f"  Interruption rate:         {scores.get('interruption_rate', 'n/a')}")
print(f"  Pass@1:                    {scores.get('pass_at_1', 'n/a')}")
PYEOF
