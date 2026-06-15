#!/usr/bin/env bash
# bit14_ab.sh -- A/B pgbench harness for the ItemPointerSIUMaybeStaleFlag
# ("bit14") fix specifically.  Uses the same two variants build.sh already
# produced under $BENCH/{master,tepid}: "master" = bit14 REVERTED (tepid-
# bit14-off), "tepid" = bit14 present (tepid-bit14-on).  Both otherwise
# identical -- this isolates exactly the fix's cost/benefit, not the SIU
# feature's own win (that's what run.sh's other workloads already cover).
#
# Runs bitmap_and_mixed.sql (80% BitmapAnd reads across two untouched
# indexes, 20% HOT-indexed updates on a third index on the same table/pages)
# A/B alternating, N iterations each, writing raw per-iteration TPS + latency
# rows to a CSV (compute the median downstream from the collected rows).
set -euo pipefail

BENCH=${BENCH:-/scratch/siu-bench}
SCALE=${SCALE:-10}
CLIENTS=${CLIENTS:-16}
THREADS=${THREADS:-8}
DURATION=${DURATION:-60}
ITERATIONS=${ITERATIONS:-5}
PORT=${PORT:-57481}
SHARED_BUFFERS=${SHARED_BUFFERS:-4GB}

TS=$(date -u +%Y%m%dT%H%M%SZ)
OUT=$BENCH/results/bit14_$TS.csv
LOGDIR=$BENCH/logs/bit14_$TS
mkdir -p "$LOGDIR" "$BENCH/results"
echo "iteration,variant,tps,latency_avg_ms" > "$OUT"
echo "=== bit14 A/B run $TS -> $OUT (scale=$SCALE clients=$CLIENTS threads=$THREADS duration=${DURATION}s iterations=$ITERATIONS)"

bin_of() { echo "$BENCH/$1/usr/local/pgsql/bin"; }
LD_of() {
  local base=$BENCH/$1/usr/local/pgsql
  if [ -d "$base/lib64" ]; then echo "$base/lib64"; else echo "$base/lib"; fi
}
psql_as() {
  local v=$1; shift
  LD_LIBRARY_PATH="$(LD_of "$v")" "$(bin_of "$v")/psql" -h /tmp -p "$PORT" -U postgres -X "$@"
}
pgbench_as() {
  local v=$1; shift
  LD_LIBRARY_PATH="$(LD_of "$v")" "$(bin_of "$v")/pgbench" -h /tmp -p "$PORT" -U postgres "$@"
}

start_pg() {
  local v=$1
  local datadir=$BENCH/_data_bit14_$v
  rm -rf "$datadir"
  mkdir -p "$datadir"
  LD_LIBRARY_PATH="$(LD_of "$v")" "$(bin_of "$v")/initdb" -D "$datadir" -U postgres >"$LOGDIR/initdb_$v.log" 2>&1
  cat >> "$datadir/postgresql.conf" <<EOF
shared_buffers = $SHARED_BUFFERS
work_mem = 32MB
max_wal_size = 4GB
synchronous_commit = on
checkpoint_timeout = 10min
wal_level = replica
logging_collector = off
port = $PORT
EOF
  LD_LIBRARY_PATH="$(LD_of "$v")" "$(bin_of "$v")/pg_ctl" -D "$datadir" \
    -o "-p $PORT" -l "$LOGDIR/pg_$v.log" start >/dev/null
  sleep 2
}

stop_pg() {
  local v=$1
  local datadir=$BENCH/_data_bit14_$v
  LD_LIBRARY_PATH="$(LD_of "$v")" "$(bin_of "$v")/pg_ctl" -D "$datadir" stop -m fast >/dev/null 2>&1 || true
}

seed() {
  local v=$1
  local rows=$((SCALE * 100000))
  psql_as "$v" <<SQL
DROP TABLE IF EXISTS siu_table;
CREATE TABLE siu_table(a int PRIMARY KEY, b int, c int, d int, e text);
CREATE INDEX siu_b ON siu_table(b);
CREATE INDEX siu_c ON siu_table(c);
CREATE INDEX siu_d ON siu_table(d);
INSERT INTO siu_table
  SELECT i, i, i, i, repeat('x', 20) FROM generate_series(1, $rows) AS i;
VACUUM (FULL, ANALYZE) siu_table;
CHECKPOINT;
SQL
}

run_iter() {
  local iter=$1 v=$2
  stop_pg "$v" || true
  start_pg "$v"
  seed "$v"
  local log="$LOGDIR/pgbench_${v}_iter${iter}.log"
  pgbench_as "$v" -n -f "$BENCH/scripts/bitmap_and_mixed.sql" \
    -c "$CLIENTS" -j "$THREADS" -T "$DURATION" -M prepared \
    -D scale="$SCALE" postgres >"$log" 2>&1 || echo "  iter $iter $v: pgbench exited nonzero, see $log" >&2
  local tps lat
  tps=$(grep -oP 'tps = \K[0-9.]+' "$log" | tail -1)
  lat=$(grep -oP 'latency average = \K[0-9.]+' "$log" | tail -1)
  if [ -z "$tps" ] || [ -z "$lat" ]; then
    # Do not write a partial/failed iteration into the result set: an NA row
    # would silently pollute the A/B comparison.  Count and report it instead.
    echo "  iter $iter  $v  FAILED (no tps/lat in $log); skipping CSV row" >&2
    stop_pg "$v"
    return
  fi
  echo "$iter,$v,$tps,$lat" >> "$OUT"
  echo "  iter $iter  $v  tps=$tps  lat=${lat}ms"
  stop_pg "$v"
}

# A/B alternate, not batched, to cancel drift.
for i in $(seq 1 "$ITERATIONS"); do
  run_iter "$i" master   # bit14-off
  run_iter "$i" tepid    # bit14-on
done

echo "=== done -> $OUT"
