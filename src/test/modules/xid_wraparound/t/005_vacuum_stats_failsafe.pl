# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# Test that the wraparound failsafe counter exposed by the extended vacuum
# statistics views advances when a VACUUM engages the wraparound failsafe.
#
# The failsafe only triggers once a relation's age exceeds
# max(vacuum_failsafe_age, autovacuum_freeze_max_age * 1.05), which cannot be
# reached by an ordinary regression test.  Here we lower
# autovacuum_freeze_max_age to its minimum and use the xid_wraparound
# extension to burn enough transaction IDs to age the table past that
# threshold, then check that the wraparound_failsafe counters advance.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if (!$ENV{PG_TEST_EXTRA} || $ENV{PG_TEST_EXTRA} !~ /\bxid_wraparound\b/)
{
	plan skip_all => "test xid_wraparound not enabled in PG_TEST_EXTRA";
}

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf(
	'postgresql.conf', qq[
autovacuum = off
track_vacuum_statistics = on
# Lower the wraparound-failsafe threshold as far as possible so that a modest
# number of consumed XIDs is enough to engage the failsafe.
autovacuum_freeze_max_age = 100000
vacuum_failsafe_age = 0
]);
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION xid_wraparound');

# A table whose relfrozenxid age we will push past the failsafe threshold.
$node->safe_psql(
	'postgres', qq[
CREATE TABLE fs_tab (id int) WITH (autovacuum_enabled = off);
INSERT INTO fs_tab SELECT generate_series(1, 1000);
]);

# Advance the XID counter well past the failsafe threshold.
$node->safe_psql('postgres', 'SELECT consume_xids(200000)');

# This VACUUM must engage the wraparound failsafe.
$node->safe_psql('postgres', 'VACUUM fs_tab');

# The per-table view records that the failsafe was engaged for this relation.
my $tab = $node->safe_psql(
	'postgres', qq[
SELECT wraparound_failsafe > 0
  FROM pg_stat_vacuum_tables WHERE relname = 'fs_tab']);
is($tab, 't', 'wraparound_failsafe advanced in pg_stat_vacuum_tables');

# The per-database aggregate counts the failsafe as well.
my $db = $node->safe_psql(
	'postgres', qq[
SELECT wraparound_failsafe > 0
  FROM pg_stat_vacuum_database WHERE dbname = current_database()]);
is($db, 't', 'wraparound_failsafe advanced in pg_stat_vacuum_database');

$node->stop;
done_testing();
