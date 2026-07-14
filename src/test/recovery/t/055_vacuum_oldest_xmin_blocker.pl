# Copyright (c) 2026, PostgreSQL Global Development Group

# Test that VACUUM reports the category of what holds the oldest xmin horizon
# back. The category is a best-effort hint, so we only assert the expected
# category, not any exact transaction or slot.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init(allows_streaming => 'logical');
$node->append_conf('postgresql.conf', qq{
autovacuum = off
max_prepared_transactions = 5
});
$node->start;

$node->safe_psql('postgres',
	"CREATE TABLE t (id int); INSERT INTO t VALUES (1)");

# Advance a few XIDs so a concurrent transaction's xmin is older than this
# session's and the blocker gets reported.
sub advance_xids
{
	$node->safe_psql('postgres',
		"SELECT txid_current() FROM generate_series(1, 5)");
}

# Vacuum a relation and return the output that names the blocker category.
sub vacuum_verbose
{
	my $rel = shift // 't';
	my ($stdout, $stderr);
	advance_xids();
	$node->psql('postgres', "VACUUM (VERBOSE) $rel",
		stdout => \$stdout, stderr => \$stderr);
	return $stderr;
}

# Nothing holds the horizon back: no blocker line.
{
	my $log = vacuum_verbose();
	unlike($log, qr/oldest xmin held back by/,
		'no blocker reported when nothing holds the horizon back');
}

# Running transaction.
{
	my $bg = $node->background_psql('postgres');
	$bg->query_safe("BEGIN");
	$bg->query_safe("SELECT txid_current()");

	my $log = vacuum_verbose();
	like($log, qr/oldest xmin held back by: running transaction/,
		'blocker reported: running transaction');

	$bg->query_safe("ROLLBACK");
	$bg->quit;
}

# Prepared transaction.
{
	$node->safe_psql('postgres', qq{
		BEGIN;
		INSERT INTO t VALUES (2);
		PREPARE TRANSACTION 'hold_xmin';
	});

	my $log = vacuum_verbose();
	like($log, qr/oldest xmin held back by: prepared transaction/,
		'blocker reported: prepared transaction');

	$node->safe_psql('postgres', "ROLLBACK PREPARED 'hold_xmin'");
}

# Logical slot's catalog_xmin: not reported for a plain user table, but
# reported for a catalog relation.
{
	$node->safe_psql('postgres',
		"SELECT pg_create_logical_replication_slot('s', 'test_decoding')");

	# Creating the slot reserves its catalog_xmin synchronously; make sure it
	# is set before relying on it as the cause.
	is($node->safe_psql('postgres',
			"SELECT catalog_xmin IS NOT NULL FROM pg_replication_slots WHERE slot_name = 's'"),
		't', 'logical slot has a catalog_xmin');

	my $log = vacuum_verbose();
	unlike($log, qr/oldest xmin held back by: logical replication slot/,
		'user table not held back by logical slot catalog_xmin');

	my $catlog = vacuum_verbose('pg_class');
	like($catlog, qr/oldest xmin held back by: logical replication slot/,
		'catalog relation held back by logical slot catalog_xmin');

	$node->safe_psql('postgres', "SELECT pg_drop_replication_slot('s')");
}

# Physical slot with a reserved xmin.
{
	$node->safe_psql('postgres',
		"SELECT pg_create_physical_replication_slot('ps', true)");

	$node->backup('backup1');
	my $standby = PostgreSQL::Test::Cluster->new('standby');
	$standby->init_from_backup($node, 'backup1', has_streaming => 1);
	$standby->append_conf('postgresql.conf', qq{
primary_slot_name = 'ps'
hot_standby_feedback = on
wal_receiver_status_interval = 1
});
	$standby->start;

	$node->poll_query_until('postgres', qq{
		SELECT xmin IS NOT NULL FROM pg_replication_slots
		WHERE slot_name = 'ps';
	}) or die "timed out waiting for slot xmin";

	# Stop the standby so only the slot's frozen xmin holds the horizon.
	$standby->stop;

	my $log = vacuum_verbose();
	like($log, qr/oldest xmin held back by: replication slot/,
		'blocker reported: replication slot');

	$node->safe_psql('postgres', "SELECT pg_drop_replication_slot('ps')");
}

# hot_standby_feedback without a slot: held by the walsender proc on the
# primary, reported as standby feedback rather than a slot.
{
	$node->backup('backup2');
	my $standby = PostgreSQL::Test::Cluster->new('standby_fb');
	$standby->init_from_backup($node, 'backup2', has_streaming => 1);
	$standby->append_conf('postgresql.conf', qq{
hot_standby_feedback = on
wal_receiver_status_interval = 1
});
	$standby->start;

	# Hold an old snapshot on the standby so its feedback pins an xmin.
	my $bg = $standby->background_psql('postgres');
	$bg->query_safe("BEGIN ISOLATION LEVEL REPEATABLE READ");
	$bg->query_safe("SELECT 1");

	$node->poll_query_until('postgres', qq{
		SELECT backend_xmin IS NOT NULL FROM pg_stat_replication;
	}) or die "timed out waiting for standby feedback xmin";

	my $log = vacuum_verbose();
	like($log, qr/oldest xmin held back by: standby feedback/,
		'blocker reported: standby feedback');

	$bg->query_safe("ROLLBACK");
	$bg->quit;
	$standby->stop;
}

$node->stop;
done_testing();
