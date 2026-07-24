# Copyright (c) 2026, PostgreSQL Global Development Group

# Test that VACUUM reports the category of what holds the oldest xmin horizon
# back. The category is a best-effort hint, so we only assert the expected
# category, not any exact transaction or slot.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Consuming this many XIDs pushes the horizon past autovacuum_freeze_max_age,
# so VACUUM emits the warning that names the specific blocker.
my $freeze_max_age = 100000;
my $xids_to_consume = $freeze_max_age + 10000;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init(allows_streaming => 'logical');
$node->append_conf('postgresql.conf', qq{
autovacuum = off
autovacuum_freeze_max_age = $freeze_max_age
max_prepared_transactions = 5
});
$node->start;

$node->safe_psql('postgres', qq{
	CREATE TABLE t (id int);
	INSERT INTO t VALUES (1);
	CREATE PROCEDURE consume_xids(n int) AS \$\$
	BEGIN
	    FOR i IN 1..n LOOP PERFORM txid_current(); COMMIT; END LOOP;
	END \$\$ LANGUAGE plpgsql;
});

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

# Push the horizon far into the past and VACUUM, so the warning fires and names
# the specific blocker. Returns the server log written during the VACUUM.
sub vacuum_far_in_past
{
	my $rel = shift // 't';
	my $logstart = -s $node->logfile;
	$node->safe_psql('postgres', "CALL consume_xids($xids_to_consume)");
	$node->safe_psql('postgres', "VACUUM $rel");
	return slurp_file($node->logfile, $logstart);
}

# Reset relfrozenxid so a prior holder doesn't keep the warning firing.
sub reset_table
{
	$node->safe_psql('postgres', "VACUUM FREEZE t");
}

# Open n backends that hold a snapshot whose xmin is the current oldest xid,
# without owning that xid themselves. The blocker that owns the xid must be
# reported ahead of these. Returns the session handles.
sub open_snapshot_holders
{
	my ($n) = @_;
	my @sessions;

	for (1 .. $n)
	{
		my $s = $node->background_psql('postgres');
		$s->query_safe("BEGIN ISOLATION LEVEL REPEATABLE READ");
		$s->query_safe("SELECT 1");
		push @sessions, $s;
	}
	return @sessions;
}

# Nothing holds the horizon back: no blocker line.
{
	my $log = vacuum_verbose();
	unlike($log, qr/oldest xmin held back by/,
		'no blocker reported when nothing holds the horizon back');
}

# Running transaction: named by its backend PID. It owns the oldest xid; the
# other backends only see that xid as their xmin, so the one that owns it must
# be reported ahead of them, whatever the proc array order.
{
	my $bg = $node->background_psql('postgres');
	$bg->query_safe("BEGIN");
	$bg->query_safe("SELECT txid_current()");
	my $pid = $bg->query_safe("SELECT pg_backend_pid()");

	my @holders = open_snapshot_holders(3);

	my $log = vacuum_far_in_past();
	like($log,
		qr/held back by: running transaction \(pid $pid\)/,
		'blocker reported: the xid owner, not a snapshot holder');

	$_->query_safe("ROLLBACK"), $_->quit for @holders;
	$bg->query_safe("ROLLBACK");
	$bg->quit;
	reset_table();
}

# Prepared transaction: named by its identifier. Like the running case, its own
# xid is the oldest; the other backends only see that xid as their xmin. The
# prepared xact must be reported ahead of them, otherwise the wrong category
# (running transaction) and PID would be reported.
{
	$node->safe_psql('postgres', qq{
		BEGIN;
		INSERT INTO t VALUES (2);
		PREPARE TRANSACTION 'hold_xmin';
	});

	my @holders = open_snapshot_holders(3);

	my $log = vacuum_far_in_past();
	like($log,
		qr/held back by: prepared transaction 'hold_xmin'/,
		'blocker reported: the prepared xact, not a snapshot holder');

	$_->query_safe("ROLLBACK"), $_->quit for @holders;
	$node->safe_psql('postgres', "ROLLBACK PREPARED 'hold_xmin'");
	reset_table();
}

# Logical slot's catalog_xmin: not reported for a plain user table, but names
# the slot for a catalog relation.
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

	my $catlog = vacuum_far_in_past('pg_class');
	like($catlog,
		qr/held back by: logical replication slot \(slot "s"\)/,
		'catalog relation held back by logical slot, slot named');

	$node->safe_psql('postgres', "SELECT pg_drop_replication_slot('s')");
	reset_table();
}

# Physical slot with a reserved xmin: named by the slot.
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

	my $log = vacuum_far_in_past();
	like($log,
		qr/held back by: replication slot \(slot "ps"\)/,
		'blocker reported: replication slot, slot named');

	$node->safe_psql('postgres', "SELECT pg_drop_replication_slot('ps')");
	reset_table();
}

# hot_standby_feedback without a slot: held by the walsender proc on the
# primary, so named by the walsender PID, noting the xact runs on the standby.
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
	my $wspid = $node->safe_psql('postgres',
		"SELECT pid FROM pg_stat_replication LIMIT 1");

	my $log = vacuum_far_in_past();
	like($log,
		qr/held back by: standby feedback \(walsender pid $wspid; the transaction runs on the standby\)/,
		'blocker reported: standby feedback with walsender PID');

	$bg->query_safe("ROLLBACK");
	$bg->quit;
	$standby->stop;
	reset_table();
}

$node->stop;
done_testing();
