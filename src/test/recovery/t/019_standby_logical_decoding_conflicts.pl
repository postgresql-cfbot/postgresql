# logical decoding on a standby : test conflict recovery; and other tests that
# verify slots get dropped as expected.

use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 24;
use RecursiveCopy;
use File::Copy;
use Time::HiRes qw(usleep);

my ($stdin, $stdout, $stderr, $ret, $handle, $slot);

my $node_master = get_new_node('master');
my $node_standby = get_new_node('standby');

# Name for the physical slot on master
my $master_slotname = 'master_physical';

# Fetch xmin columns from slot's pg_replication_slots row, after waiting for
# given boolean condition to be true to ensure we've reached a quiescent state
sub wait_for_xmins
{
	my ($node, $slotname, $check_expr) = @_;

	$node->poll_query_until(
		'postgres', qq[
		SELECT $check_expr
		FROM pg_catalog.pg_replication_slots
		WHERE slot_name = '$slotname';
	]) or die "Timed out waiting for slot xmins to advance";
}

# Create the required logical slots on standby.
sub create_logical_slots
{
	$node_standby->create_logical_slot_on_standby($node_master, 'dropslot', 'testdb');
	$node_standby->create_logical_slot_on_standby($node_master, 'activeslot', 'testdb');
}

# Acquire one of the standby logical slots created by create_logical_slots()
sub make_slot_active
{
	my $slot_user_handle;

	# make sure activeslot is in use
	print "starting pg_recvlogical\n";
	$slot_user_handle = IPC::Run::start(['pg_recvlogical', '-d', $node_standby->connstr('testdb'), '-S', 'activeslot', '-f', '-', '--no-loop', '--start'], '>', \$stdout, '2>', \$stderr);

	while (!$node_standby->slot('activeslot')->{'active_pid'})
	{
		usleep(100_000);
		print "waiting for slot to become active\n";
	}
	return $slot_user_handle;
}

# Check if all the slots on standby are dropped. These include the 'activeslot'
# that was acquired by make_slot_active(), and the non-active 'dropslot'.
sub check_slots_dropped
{
	my ($slot_user_handle) = @_;
	my $return;

	is($node_standby->slot('dropslot')->{'slot_type'}, '', 'dropslot on standby dropped');
	is($node_standby->slot('activeslot')->{'slot_type'}, '', 'activeslot on standby dropped');

	# our client should've terminated in response to the walsender error
	eval {
		$slot_user_handle->finish;
	};
	$return = $?;
	cmp_ok($return, "!=", 0, "pg_recvlogical exited non-zero\n");
	if ($return) {
		like($stderr, qr/conflict with recovery/, 'recvlogical recovery conflict');
		like($stderr, qr/must be dropped/, 'recvlogical error detail');
	}

	return 0;
}


########################
# Initialize master node
########################

$node_master->init(allows_streaming => 1, has_archiving => 1);
$node_master->append_conf('postgresql.conf', q{
wal_level = 'logical'
max_replication_slots = 4
max_wal_senders = 4
log_min_messages = 'debug2'
log_error_verbosity = verbose
# send status rapidly so we promptly advance xmin on master
wal_receiver_status_interval = 1
# very promptly terminate conflicting backends
max_standby_streaming_delay = '2s'
});
$node_master->dump_info;
$node_master->start;

$node_master->psql('postgres', q[CREATE DATABASE testdb]);

$node_master->safe_psql('testdb', qq[SELECT * FROM pg_create_physical_replication_slot('$master_slotname');]);
my $backup_name = 'b1';
$node_master->backup($backup_name);

#######################
# Initialize slave node
#######################

$node_standby->init_from_backup(
	$node_master, $backup_name,
	has_streaming => 1,
	has_restoring => 1);
$node_standby->append_conf('postgresql.conf',
	qq[primary_slot_name = '$master_slotname']);
$node_standby->start;
$node_master->wait_for_catchup($node_standby, 'replay', $node_master->lsn('flush'));


##################################################
# Recovery conflict: Drop conflicting slots, including in-use slots
# Scenario 1 : hot_standby_feedback off
##################################################

create_logical_slots();

# One way to reproduce recovery conflict is to run VACUUM FULL with
# hot_standby_feedback turned off on slave.
$node_standby->append_conf('postgresql.conf',q[
hot_standby_feedback = off
]);
$node_standby->restart;
# ensure walreceiver feedback off by waiting for expected xmin and
# catalog_xmin on master. Both should be NULL since hs_feedback is off
wait_for_xmins($node_master, $master_slotname,
			   "xmin IS NULL AND catalog_xmin IS NULL");

$handle = make_slot_active();

# This should trigger the conflict
$node_master->safe_psql('testdb', 'VACUUM FULL');

$node_master->wait_for_catchup($node_standby, 'replay', $node_master->lsn('flush'));

check_slots_dropped($handle);

# Turn hot_standby_feedback back on
$node_standby->append_conf('postgresql.conf',q[
hot_standby_feedback = on
]);
$node_standby->restart;

# ensure walreceiver feedback sent by waiting for expected xmin and
# catalog_xmin on master. With hot_standby_feedback on, xmin should advance,
# but catalog_xmin should still remain NULL since there is no logical slot.
wait_for_xmins($node_master, $master_slotname,
			   "xmin IS NOT NULL AND catalog_xmin IS NULL");

##################################################
# Recovery conflict: Drop conflicting slots, including in-use slots
# Scenario 2 : incorrect wal_level at master
##################################################

create_logical_slots();

$handle = make_slot_active();

# Make master wal_level replica. This will trigger slot conflict.
$node_master->append_conf('postgresql.conf',q[
wal_level = 'replica'
]);
$node_master->restart;

$node_master->wait_for_catchup($node_standby, 'replay', $node_master->lsn('flush'));

check_slots_dropped($handle);

# Restore master wal_level
$node_master->append_conf('postgresql.conf',q[
wal_level = 'logical'
]);
$node_master->restart;
$node_master->wait_for_catchup($node_standby, 'replay', $node_master->lsn('flush'));


##################################################
# DROP DATABASE should drops it's slots, including active slots.
##################################################

create_logical_slots();
$handle = make_slot_active();

# Create a slot on a database that would not be dropped. This slot should not
# get dropped.
$node_standby->create_logical_slot_on_standby($node_master, 'otherslot', 'postgres');

# dropdb on the master to verify slots are dropped on standby
$node_master->safe_psql('postgres', q[DROP DATABASE testdb]);

$node_master->wait_for_catchup($node_standby, 'replay', $node_master->lsn('flush'));

is($node_standby->safe_psql('postgres',
	q[SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = 'testdb')]), 'f',
	'database dropped on standby');

check_slots_dropped($handle);

is($node_standby->slot('otherslot')->{'slot_type'}, 'logical',
	'otherslot on standby not dropped');

# Cleanup : manually drop the slot that was not dropped.
$node_standby->psql('postgres', q[SELECT pg_drop_replication_slot('otherslot')]);
