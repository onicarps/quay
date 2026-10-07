#!/usr/bin/env bash
# Quay end-to-end demo.
#
# Runs unattended, touches nothing outside a temporary directory, and exits 0
# only when every step succeeded. Uses the source tree, so no install is needed.
#
#   ./run_demo.sh              # full run
#   ./run_demo.sh --keep       # keep the scratch directory and print its path
#   ./run_demo.sh --quick      # skip the benchmark step

set -euo pipefail

KEEP=0
RUN_BENCH=1
for arg in "$@"; do
    case "$arg" in
        --keep) KEEP=1 ;;
        --quick) RUN_BENCH=0 ;;
        -h|--help)
            sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "unknown option: $arg" >&2
            exit 2
            ;;
    esac
done

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="$HERE/src"
if [[ ! -d "$SRC/quay" ]]; then
    echo "cannot find src/quay next to this script (looked in $SRC)" >&2
    exit 1
fi

SCRATCH="$(mktemp -d "${TMPDIR:-/tmp}/quay-demo-XXXXXX")"
WORKSPACE="$SCRATCH/workspace"
export XDG_CONFIG_HOME="$SCRATCH/config"      # keeps the real registry untouched
export PYTHONPATH="$SRC${PYTHONPATH:+:$PYTHONPATH}"
mkdir -p "$WORKSPACE" "$XDG_CONFIG_HOME"

cleanup() {
    local status=$?
    for root in "$WORKSPACE" "$SCRATCH/tamper"; do
        if [[ -f "$root/.quay/quayd.pid" ]]; then
            python3 -m quay.cli stop --workspace "$root" --no-json >/dev/null 2>&1 || true
        fi
    done
    if [[ $KEEP -eq 1 ]]; then
        echo
        echo "scratch directory kept: $SCRATCH"
    else
        rm -rf "$SCRATCH"
    fi
    exit $status
}
trap cleanup EXIT

QUAY=(python3 -m quay.cli --workspace "$WORKSPACE")

step() {
    echo
    echo "== $* =="
}

fail() {
    echo "DEMO FAILED: $*" >&2
    exit 1
}

step "1. Versions and environment"
python3 -c 'import sys; print("python", sys.version.split()[0])'
python3 -m quay.cli --version
python3 -m quay.daemon --version
echo "workspace: $WORKSPACE"

step "2. Initialise the workspace"
"${QUAY[@]}" init --no-json

step "3. Append a few events"
"${QUAY[@]}" emit build/log '{"text": "configure ok", "tags": ["build"]}' --kind note --producer demo --no-json
"${QUAY[@]}" emit build/log '{"text": "compiled ok"}' --kind note --producer demo --no-json
"${QUAY[@]}" emit build/alert '{"summary": "disk almost full"}' --kind signal --producer demo --no-json
"${QUAY[@]}" emit build/metric '{"name": "build_seconds", "value": 12.5}' --kind metric --producer demo --no-json

step "4. Drive the work plane"
"${QUAY[@]}" work claim job-42 --executor builder --lease-seconds 60 --no-json
"${QUAY[@]}" work heartbeat job-42 --executor builder --lease-seconds 60 --no-json
"${QUAY[@]}" work complete job-42 --executor builder \
    --evidence '{"artifact": "dist/app.tar.gz", "exit_status": 0}' --no-json
"${QUAY[@]}" work list --state done --no-json

step "5. Read it back: log, query, exports"
"${QUAY[@]}" log --limit 3 --no-json
"${QUAY[@]}" query --topic 'build/*' --kind note --count-only --no-json
"${QUAY[@]}" query --kind work --state done --export jsonl | head -1
"${QUAY[@]}" query --kind work --state done --export csv | head -2
"${QUAY[@]}" query --text 'almost full' --count-only --no-json

step "6. Status is O(1), not a scan"
"${QUAY[@]}" status --no-json

step "7. Live streaming to a subscriber"
"${QUAY[@]}" watch --plain --history 0 --max-events 2 --for 20 >"$SCRATCH/stream.jsonl" &
STREAM_PID=$!
sleep 1
"${QUAY[@]}" emit demo/live '{"n": 1}' --producer demo --no-json >/dev/null
"${QUAY[@]}" emit demo/live '{"n": 2}' --producer demo --no-json >/dev/null
wait "$STREAM_PID" || fail "the streaming subscriber exited non-zero"
STREAMED=$(wc -l <"$SCRATCH/stream.jsonl" | tr -d ' ')
[[ "$STREAMED" == "2" ]] || fail "expected 2 streamed events, saw $STREAMED"
echo "streamed $STREAMED events to the subscriber"

step "8. Verify the hash chain"
"${QUAY[@]}" verify --no-json

step "9. Tamper detection (in a separate throwaway workspace)"
TAMPER_WS="$SCRATCH/tamper"
mkdir -p "$TAMPER_WS"
TQUAY=(python3 -m quay.cli --workspace "$TAMPER_WS")
"${TQUAY[@]}" emit demo/tamper '{"text": "honest"}' --kind note --producer demo --no-json >/dev/null
"${TQUAY[@]}" verify --no-json
python3 - "$TAMPER_WS/.quay/ledger.db" <<'PY'
import sqlite3, sys
connection = sqlite3.connect(sys.argv[1])
row = connection.execute("SELECT seq, payload FROM events ORDER BY seq LIMIT 1").fetchone()
connection.execute("UPDATE events SET payload = ? WHERE seq = ?", ('{"text": "tampered"}', row[0]))
connection.commit()
connection.close()
print(f"rewrote the payload of event #{row[0]} directly in SQLite")
PY
if "${TQUAY[@]}" verify --json >"$SCRATCH/verify-after-tamper.json" 2>&1; then
    fail "verification passed on a tampered ledger"
fi
echo "verification correctly failed:"
python3 -c '
import json, sys
report = json.load(open(sys.argv[1]))
print("  ok:", report["ok"])
print("  divergence at seq:", (report.get("divergence") or {}).get("seq"))
' "$SCRATCH/verify-after-tamper.json"
"${TQUAY[@]}" stop --no-json >/dev/null

step "10. Import a Changi ledger"
python3 - "$SCRATCH/changi" <<'PY'
import json, os, sqlite3, sys
root = sys.argv[1]
os.makedirs(root, exist_ok=True)
connection = sqlite3.connect(os.path.join(root, "events.db"))
connection.executescript(
    "CREATE TABLE events (event_id INTEGER PRIMARY KEY AUTOINCREMENT, topic TEXT NOT NULL,"
    " producer_id TEXT NOT NULL, timestamp TEXT NOT NULL, schema_version INTEGER NOT NULL DEFAULT 1,"
    " payload TEXT NOT NULL, causation_id INTEGER);"
)
rows = [
    ("changi/build", "changi.worker", {"message": "legacy note"}),
    ("changi/alert", "changi.monitor", {"summary": "legacy alert", "changi": {"kind": "signal", "state": "claimed"}}),
    ("changi/receipt", "changi.worker", {"changi": {"kind": "work_receipt"},
     "work_receipt": {"executor_id": "legacy-1", "receipt_id": "old-job", "exit_status": 0}}),
]
for index, (topic, producer, payload) in enumerate(rows, start=1):
    connection.execute(
        "INSERT INTO events (topic, producer_id, timestamp, payload) VALUES (?, ?, ?, ?)",
        (topic, producer, f"2026-10-04T12:00:0{index}+00:00", json.dumps(payload)),
    )
connection.commit()
connection.close()
print(f"wrote {len(rows)} Changi events to {root}/events.db")
PY
"${QUAY[@]}" import "$SCRATCH/changi" --dry-run --no-json
"${QUAY[@]}" import "$SCRATCH/changi" --no-json
"${QUAY[@]}" query --topic 'changi/*' --count-only --no-json

step "11. Doctor and daemon registry"
"${QUAY[@]}" doctor --no-json
"${QUAY[@]}" daemons --no-json

if [[ $RUN_BENCH -eq 1 ]]; then
    step "12. Benchmarks (quick profile)"
    "${QUAY[@]}" bench --scale quick --json | python3 -c '
import json, sys
report = json.load(sys.stdin)
print("profile:", json.dumps(report["profile"], sort_keys=True))
for result in report["results"]:
    print("- %s: %s" % (result["name"], result["notes"]))
'
else
    step "12. Benchmarks skipped (--quick)"
fi

step "13. Stop the daemon"
"${QUAY[@]}" stop --no-json

echo
echo "DEMO PASSED"
