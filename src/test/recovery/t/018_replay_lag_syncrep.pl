# Test impact of replay lag on synchronous replication.
#
# Replay lag is induced using recovery_min_apply_delay GUC.  Two ways
# of breaking replication connection are covered - killing walsender
# and restarting standby.  The test expects that replication
# connection is restored without being affected due to replay lag.
# This is validated by performing commits on master after replication
# connection is disconnected and checking that they finish within a
# few seconds.

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 5;

# Query checking sync_priority and sync_state of each standby
my $check_sql =
  "SELECT application_name, sync_priority, sync_state FROM pg_stat_replication ORDER BY application_name;";

# Check that sync_state of a standby is expected (waiting till it is).
# If $setting is given, synchronous_standby_names is set to it and
# the configuration file is reloaded before the test.
sub test_sync_state
{
	my ($self, $expected, $msg, $setting) = @_;

	if (defined($setting))
	{
		$self->safe_psql('postgres',
						 "ALTER SYSTEM SET synchronous_standby_names = '$setting';");
		$self->reload;
	}

	ok($self->poll_query_until('postgres', $check_sql, $expected), $msg);
	return;
}

# Start a standby and check that it is registered within the WAL sender
# array of the given primary.  This polls the primary's pg_stat_replication
# until the standby is confirmed as registered.
sub start_standby_and_wait
{
	my ($master, $standby) = @_;
	my $master_name  = $master->name;
	my $standby_name = $standby->name;
	my $query =
	  "SELECT count(1) = 1 FROM pg_stat_replication WHERE application_name = '$standby_name'";

	$standby->start;

	print("### Waiting for standby \"$standby_name\" on \"$master_name\"\n");
	$master->poll_query_until('postgres', $query);
	return;
}

# Initialize master node
my $node_master = get_new_node('master');
my @extra = (q[--wal-segsize], q[1]);
$node_master->init(allows_streaming => 1, extra => \@extra);
$node_master->start;
my $backup_name = 'master_backup';

# Setup physical replication slot for streaming replication
$node_master->safe_psql('postgres',
	q[SELECT pg_create_physical_replication_slot('phys_slot', true, false);]);

# Take backup
$node_master->backup($backup_name);

# Create standby linking to master
my $node_standby = get_new_node('standby');
$node_standby->init_from_backup($node_master, $backup_name,
								has_streaming => 1);
$node_standby->append_conf('postgresql.conf',
						   q[primary_slot_name = 'phys_slot']);
$node_standby->append_conf('postgresql.conf',
						   q[wal_receiver_start_condition = 'consistency']);
# Enable debug logging in standby
$node_standby->append_conf('postgresql.conf',
						   q[log_min_messages = debug5]);

start_standby_and_wait($node_master, $node_standby);

# Make standby synchronous
test_sync_state(
	$node_master,
	qq(standby|1|sync),
	'standby is synchronous',
	'standby');

# Switch to a new WAL file after standby is created.  This gives the
# standby a chance to save the new WAL file's beginning as replication
# start point.
$node_master->safe_psql('postgres',	'create table dummy(a int);');
$node_master->safe_psql(
	'postgres',
	'select pg_switch_wal();');

# Wait for standby to replay all WAL.
$node_master->wait_for_catchup('standby', 'replay',
							   $node_master->lsn('insert'));

# Slow down WAL replay by inducing 10 seconds sleep before replaying
# a commit WAL record.
$node_standby->safe_psql('postgres',
						 'ALTER SYSTEM set recovery_min_apply_delay TO 10000;');
$node_standby->reload;

# Commit some transactions on master to induce replay lag in standby.
$node_master->safe_psql('postgres', 'CREATE TABLE replay_lag_test(a int);');
$node_master->safe_psql(
	'postgres',
	'insert into replay_lag_test values (101);');
$node_master->safe_psql(
	'postgres',
	'insert into replay_lag_test values (102);');
$node_master->safe_psql(
	'postgres',
	'insert into replay_lag_test values (103);');

# Obtain WAL sender PID and kill it.
my $walsender_pid = $node_master->safe_psql(
	'postgres',
	q[select active_pid from pg_get_replication_slots() where slot_name = 'phys_slot']);

# Kill walsender, so that the replication connection breaks.
kill 'SIGTERM', $walsender_pid;

# Wait for 10 seconds (recovery_min_apply_delay) to give startup
# process a chance to restart WAL receiver.
note('waiting for startup process to restart WAL receiver');
sleep 10;

# The replication connection should be re-establised much earlier than
# what it takes to finish replay.  Try to commit a transaction with a
# timeout of 2 seconds.  The timeout should not be hit.
my $timed_out = 0;
$node_master->safe_psql(
	'postgres',
	'insert into replay_lag_test values (1);',
	timeout => 2,
	timed_out => \$timed_out);

is($timed_out, 0, 'insert after WAL receiver restart');

# Break the replication connection by restarting standby.
$node_standby->restart;

# Like in previous test, the replication connection should be
# re-establised before pending WAL replay is finished.  Try to commit
# a transaction with 2 second timeout.  The timeout should not be hit.
$timed_out = 0;
$node_master->safe_psql(
	'postgres',
	'insert into replay_lag_test values (2);',
	timeout => 2,
	timed_out => \$timed_out);

is($timed_out, 0, 'insert after standby restart');

# Reset the delay so that the replay process is no longer slowed down.
$node_standby->safe_psql('postgres', 'ALTER SYSTEM set recovery_min_apply_delay to 0;');
$node_standby->reload;

# Switch to a new WAL file and see if things work well.
$node_master->safe_psql(
	'postgres',
	'select pg_switch_wal();');

# Transactions should work fine on master.
$timed_out = 0;
$node_master->safe_psql(
	'postgres',
	'insert into replay_lag_test values (3);',
	timeout => 1,
	timed_out => \$timed_out);

# Wait for standby to replay all WAL.
$node_master->wait_for_catchup('standby', 'replay',
							   $node_master->lsn('insert'));

# Standby should also have identical content.
my $count_sql = q[select count(*) from replay_lag_test;];
my $expected = q[6];
ok($node_standby->poll_query_until('postgres', $count_sql, $expected), 'standby query');

# Test that promotion followed by query works.
$node_standby->promote;
$node_master->stop;
$node_standby->safe_psql('postgres', 'insert into replay_lag_test values (4);');

$expected = q[7];
ok($node_standby->poll_query_until('postgres', $count_sql, $expected),
   'standby query after promotion');
