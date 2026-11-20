#!/bin/sh
# Run in an isolated Linux compute image with no storage services. Both PGDATA
# preparation and audit configuration have deterministic errors. PGDATA must
# finish first and its failure must prevent both audit and postmaster startup.
# max_connections=0 makes the real PG sync helper fail on each attempt. An
# otherwise valid configuration can keep retrying an unreachable peer forever.
set -eu
work=$(mktemp -d)
pid=
trap 'if test -n "$pid"; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi; rm -rf "$work"' EXIT
unset AUDIT_LOGGING_ENDPOINT AUDIT_LOGGING_TLS_ENDPOINT
cat > "$work/config.json" <<'JSON'
{"compute_ctl_config":{"jwks":{"keys":[]}},"spec":{"format_version":2,
"mode":"Primary","audit_log_level":"Metadata","suspend_timeout_seconds":0,
"cluster":{"roles":[],"databases":[],"settings":[{"name":"max_connections","value":"0","vartype":"integer"}]},
"tenant_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","timeline_id":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
"pageserver_connstring":"postgresql://127.0.0.1:65431/postgres","safekeeper_connstrings":["127.0.0.1:65432"]}}
JSON
"${1:-compute_ctl}" --dev --lakebase-mode false --compute-id audit-order-test --config "$work/config.json" \
    --pgbin "${2:-/usr/local/bin/postgres}" --pgdata "$work/pgdata" \
    --connstr postgresql://cloud_admin@127.0.0.1:55433/postgres >"$work/startup.log" 2>&1 &
pid=$!
for attempt in $(seq 200); do
    if grep -q 'could not start the compute node:' "$work/startup.log"; then break; fi
    if ! kill -0 "$pid" 2>/dev/null; then break; fi
    sleep 0.1
done
error=$(grep -m1 'could not start the compute node:' "$work/startup.log" || true)
case "$error" in
    *'could not start the compute node: failed to sync safekeepers'*) ;;
    *) echo "Expected PGDATA preparation to fail before audit validation. Observed: $error" >&2; exit 1 ;;
esac
grep -Fq '0 is outside the valid range for parameter "max_connections"' "$work/startup.log"
test -s "$work/pgdata/postgresql.conf"
test ! -e "$work/pgdata/postmaster.pid"
test ! -e "$work/pgdata/log/rsyslog-state"
echo 'PASS: PGDATA preparation failure precedes audit validation and postmaster startup'
