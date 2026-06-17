# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# Test that per-index vacuum statistics are collected for indexes that are
# vacuumed through the PARALLEL path.
#
# When VACUUM distributes index cleanup to parallel workers, the WAL/buffer
# usage and the tuple counters of each index are produced in a different
# process than the leader.  The sampling machinery has to gather those numbers
# from the workers and attribute them to the right index in pg_stat_vacuum_indexes
# -- otherwise the index work is silently lost (or folded into the heap's
# figures).  A regression test cannot guarantee that the parallel path is taken,
# so this is done in a TAP cluster where we can force it and verify, from
# VACUUM (VERBOSE) output, that a worker was actually launched.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf(
	'postgresql.conf', qq[
autovacuum = off
track_vacuum_statistics = on
# Make every index eligible for the parallel path and make sure workers can run.
min_parallel_index_scan_size = 0
max_parallel_maintenance_workers = 4
max_worker_processes = 8
max_parallel_workers = 8
]);
$node->start;

# Build the relation that we will vacuum in parallel.  Two indexes are needed
# so the parallel path has more than one index to distribute, and the table is
# large enough that the leftover indexes are worth scanning.
my $setup = sub {
	my ($tab) = @_;
	$node->safe_psql(
		'postgres', qq[
CREATE TABLE $tab (id int, val int) WITH (autovacuum_enabled = off);
INSERT INTO $tab SELECT g, g FROM generate_series(1, 100000) g;
CREATE INDEX ${tab}_i1 ON $tab (id);
CREATE INDEX ${tab}_i2 ON $tab (val);
DELETE FROM $tab WHERE id % 2 = 0;   -- 50000 dead index entries per index
]);
};

# Per-index statistics of $tab, with the table-specific prefix stripped from the
# index name so the parallel and serial relations can be compared directly.  We
# compare the counters that are fully determined by the index content and are
# therefore identical no matter which process did the work: the number of
# removed index entries, the number of deleted index pages and the number of WAL
# records.  (wal_fpi/wal_bytes are left out: full-page images depend on when the
# last checkpoint happened, so they legitimately differ between the two runs.)
my $index_stats = sub {
	my ($tab) = @_;
	return $node->safe_psql(
		'postgres', qq[
SELECT string_agg(
         format('%s:%s:%s:%s', replace(indexrelname, '$tab', 'tab'),
                tuples_deleted, pages_deleted, wal_records),
         ',' ORDER BY indexrelname)
  FROM pg_stat_vacuum_indexes WHERE relname = '$tab']);
};

# --- Parallel vacuum -------------------------------------------------------
$setup->('vacparstat_par');

# Run with VERBOSE so we can confirm a parallel worker was really launched;
# without that confirmation the test could silently degrade to a serial vacuum.
my ($ret, $stdout, $stderr) =
  $node->psql('postgres', 'VACUUM (VERBOSE, PARALLEL 2) vacparstat_par');
is($ret, 0, 'parallel VACUUM succeeded');
like(
	$stderr,
	qr/launched [1-9]\d* parallel vacuum worker.* for index vacuuming/,
	'parallel index vacuum actually launched a worker');

# --- Serial vacuum (control) -----------------------------------------------
# The same workload vacuumed without parallelism.
$setup->('vacparstat_ser');
$node->safe_psql('postgres', 'VACUUM (PARALLEL 0) vacparstat_ser');

$node->safe_psql('postgres', 'SELECT pg_stat_force_next_flush()');

my $par = $index_stats->('vacparstat_par');
my $ser = $index_stats->('vacparstat_ser');

# Sanity-check the parallel numbers so the equality assertion below cannot pass
# just because both paths captured nothing: 50000 removed entries and some WAL.
like(
	$par,
	qr/^tab_i1:50000:\d+:[1-9]\d*,tab_i2:50000:\d+:[1-9]\d*$/,
	'parallel index vacuum captured the expected per-index counters');

# The whole point: the parallel path must attribute exactly the same per-index
# statistics to each index as the leader-only path does.
is($par, $ser,
	'parallel and serial index vacuum capture identical per-index statistics');

$node->stop;
done_testing();
