#!/bin/sh
# Run inside an isolated compute image. A healthy PG config with no reachable
# safekeeper leaves the real sync helper waiting while compute is still in Init.
set -eu
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cat > "$work/config.json" <<'JSON'
{"compute_ctl_config":{"jwks":{"keys":[]}},"spec":{"format_version":2,
"mode":"Primary","suspend_timeout_seconds":0,"cluster":{"roles":[],"databases":[],"settings":[]},
"tenant_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","timeline_id":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
"pageserver_connstring":"postgresql://127.0.0.1:65431/postgres","safekeeper_connstrings":["127.0.0.1:65432"]}}
JSON
status=0
# The monitor checks every five seconds. The outer deadline also bounds a
# regression where it never starts and the sync helper keeps retrying.
timeout -k 2 12 "${1:-compute_ctl}" --dev --lakebase-mode true --pg-init-timeout 1 \
    --config "$work/config.json" --compute-id init-watchdog --pgdata "$work/pgdata" \
    --pgbin "${2:-/usr/local/bin/postgres}" --connstr postgresql://cloud_admin@127.0.0.1:55433/postgres \
    >"$work/startup.log" 2>&1 || status=$?
grep -Fq 'starting safekeepers syncing' "$work/startup.log"
if test "$status" != 1 || ! grep -Fq 'compute did not enter Running state in 1 seconds, exiting' "$work/startup.log"; then
    echo "Init watchdog did not exit compute_ctl while PGDATA was blocked (exit $status)" >&2
    exit 1
fi
test ! -e "$work/pgdata/postmaster.pid"
echo 'PASS: Init watchdog terminated compute_ctl while safekeeper sync was pending'
