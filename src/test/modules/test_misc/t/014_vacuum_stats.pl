# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# Test the recently_dead_tuples and missed_dead_tuples/missed_dead_pages
# counters of the extended vacuum statistics view pg_stat_vacuum_tables.
#
# These counters depend on inter-session visibility and on VACUUM's ability to
# acquire a cleanup lock, so they cannot be exercised by an ordinary
# single-session regression test.  A dedicated TAP cluster gives us full
# control over the removal horizon (no concurrent backends hold it back), which
# makes the outcome deterministic -- unlike an isolation test running against a
# shared regression cluster.

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
]);
$node->start;

# A small table that fits on a single heap page, so the deleted tuples and the
# page pinned by the cursor below are the same page.
$node->safe_psql(
	'postgres', qq[
CREATE TABLE vacstat_iso (id int, ival int) WITH (autovacuum_enabled = off);
INSERT INTO vacstat_iso SELECT i, i FROM generate_series(1, 50) i;
]);

# Helper: fetch the four interesting counters for the table.
my $stats_query = qq[
SELECT tuples_deleted, recently_dead_tuples, missed_dead_tuples, missed_dead_pages
  FROM pg_stat_vacuum_tables WHERE relname = 'vacstat_iso'];

# This session first holds an old snapshot (so the deleted tuples stay
# recently dead), and later pins the heap page (so VACUUM cannot get a cleanup
# lock and the now-removable tuples are missed instead).
my $holder = $node->background_psql('postgres', on_error_stop => 1);

# 1. Hold a repeatable-read snapshot that can still see the soon-to-be-deleted
#    tuples, preventing VACUUM from removing them.
$holder->query_safe(
	'BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;'
	  . ' SELECT count(*) FROM vacstat_iso;');

# 2. Delete ten tuples and vacuum.  They are dead but not yet removable, so
#    they are counted as recently dead and not removed.
$node->safe_psql('postgres', 'DELETE FROM vacstat_iso WHERE id <= 10;');
$node->safe_psql('postgres', 'VACUUM vacstat_iso;');
is( $node->safe_psql('postgres', $stats_query),
	"0|10|0|0",
	'recently_dead_tuples counted while an old snapshot is held');

# 3. Release the old snapshot, then pin the table's single heap page with a
#    cursor so VACUUM cannot acquire a cleanup lock on it.
$holder->query_safe('COMMIT;');
$holder->query_safe(
	'BEGIN; DECLARE c CURSOR FOR SELECT * FROM vacstat_iso;'
	  . ' FETCH NEXT FROM c;');

# 4. The deleted tuples are now removable, but the page is pinned, so a plain
#    VACUUM skips it without a cleanup lock and counts the tuples as missed.
#    (Counters accumulate, so recently_dead_tuples stays at 10.)
$node->safe_psql('postgres', 'VACUUM vacstat_iso;');
is( $node->safe_psql('postgres', $stats_query),
	"0|10|10|1",
	'missed_dead_tuples/missed_dead_pages counted while the page is pinned');

# 5. Release the pin and vacuum once more; the tuples are finally removed.
$holder->query_safe('COMMIT;');
$node->safe_psql('postgres', 'VACUUM vacstat_iso;');
is( $node->safe_psql('postgres', $stats_query),
	"10|10|10|1",
	'dead tuples removed once the pin is released');

$holder->quit;
$node->stop;
done_testing();
