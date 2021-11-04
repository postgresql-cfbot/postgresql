# logical decoding on standby : test logical decoding,
# recovery conflict and standby promotion.

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use Test::More tests => 38;
use Time::HiRes qw(usleep);

my ($stdin, $stdout, $stderr, $ret, $handle, $slot);

my $node_primary = PostgreSQL::Test::Cluster->new('primary');
my $node_standby = PostgreSQL::Test::Cluster->new('standby');

# Name for the physical slot on primary
my $primary_slotname = 'primary_physical';

# return the size of logfile of $node in bytes
sub get_log_size
{
	my ($node) = @_;

	return (stat $node->logfile)[7];
}

# find $pat in logfile of $node after $off-th byte
sub find_in_log
{
	my ($node, $pat, $off) = @_;

	$off = 0 unless defined $off;
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile);
	return 0 if (length($log) <= $off);

	$log = substr($log, $off);

	return $log =~ m/$pat/;
}

# Fetch xmin columns from slot's pg_replication_slots row, after waiting for
# given boolean condition to be true to ensure we've reached a quiescent state.
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
	$node_standby->create_logical_slot_on_standby($node_primary, 'inactiveslot', 'testdb');
	$node_standby->create_logical_slot_on_standby($node_primary, 'activeslot', 'testdb');
}

# Acquire one of the standby logical slots created by create_logical_slots().
# In case wait is true we are waiting for an active pid on the 'activeslot' slot.
# If wait is not true it means we are testing a known failure scenario.
sub make_slot_active
{
	my $wait = shift;
	my $slot_user_handle;

	print "starting pg_recvlogical\n";
	$slot_user_handle = IPC::Run::start(['pg_recvlogical', '-d', $node_standby->connstr('testdb'), '-S', 'activeslot', '-f', '-', '--no-loop', '--start'], '>', \$stdout, '2>', \$stderr);

	if ($wait)
	# make sure activeslot is in use
	{
		$node_standby->poll_query_until('testdb',
			"SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'activeslot' AND active_pid IS NOT NULL)"
		) or die "slot never became active";
	}

	return $slot_user_handle;
}

# Check if all the slots on standby are dropped. These include the 'activeslot'
# that was acquired by make_slot_active(), and the non-active 'inactiveslot'.
sub check_slots_dropped
{
	my ($slot_user_handle) = @_;
	my $return;

	is($node_standby->slot('inactiveslot')->{'slot_type'}, '', 'inactiveslot on standby dropped');
	is($node_standby->slot('activeslot')->{'slot_type'}, '', 'activeslot on standby dropped');

	# our client should've terminated in response to the walsender error
	$slot_user_handle->finish;
	$return = $?;
	cmp_ok($return, "!=", 0, "pg_recvlogical exited non-zero");
	if ($return) {
		like($stderr, qr/conflict with recovery/, 'slot have been dropped');
	}

	return 0;
}


########################
# Initialize primary node
########################

$node_primary->init(allows_streaming => 1, has_archiving => 1);
$node_primary->append_conf('postgresql.conf', q{
wal_level = 'logical'
max_replication_slots = 4
max_wal_senders = 4
log_min_messages = 'debug2'
log_error_verbosity = verbose
});
$node_primary->dump_info;
$node_primary->start;

$node_primary->psql('postgres', q[CREATE DATABASE testdb]);

$node_primary->safe_psql('testdb', qq[SELECT * FROM pg_create_physical_replication_slot('$primary_slotname');]);
my $backup_name = 'b1';
$node_primary->backup($backup_name);

#######################
# Initialize standby node
#######################

$node_standby->init_from_backup(
	$node_primary, $backup_name,
	has_streaming => 1,
	has_restoring => 1);
$node_standby->append_conf('postgresql.conf',
	qq[primary_slot_name = '$primary_slotname']);
$node_standby->start;
$node_primary->wait_for_catchup($node_standby, 'replay', $node_primary->lsn('flush'));


##################################################
# Test that logical decoding on the standby
# behaves correctly.
##################################################

create_logical_slots();

$node_primary->safe_psql('testdb', qq[CREATE TABLE decoding_test(x integer, y text);]);
$node_primary->safe_psql('testdb', qq[INSERT INTO decoding_test(x,y) SELECT s, s::text FROM generate_series(1,10) s;]);

$node_primary->wait_for_catchup($node_standby, 'replay', $node_primary->lsn('flush'));

my $result = $node_standby->safe_psql('testdb',
	qq[SELECT pg_logical_slot_get_changes('activeslot', NULL, NULL);]);

# test if basic decoding works
is(scalar(my @foobar = split /^/m, $result),
	14, 'Decoding produced 14 rows');

# Insert some rows and verify that we get the same results from pg_recvlogical
# and the SQL interface.
$node_primary->safe_psql('testdb',
	qq[INSERT INTO decoding_test(x,y) SELECT s, s::text FROM generate_series(1,4) s;]
);

my $expected = q{BEGIN
table public.decoding_test: INSERT: x[integer]:1 y[text]:'1'
table public.decoding_test: INSERT: x[integer]:2 y[text]:'2'
table public.decoding_test: INSERT: x[integer]:3 y[text]:'3'
table public.decoding_test: INSERT: x[integer]:4 y[text]:'4'
COMMIT};

$node_primary->wait_for_catchup($node_standby, 'replay', $node_primary->lsn('flush'));

my $stdout_sql = $node_standby->safe_psql('testdb',
	qq[SELECT data FROM pg_logical_slot_peek_changes('activeslot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');]
);

is($stdout_sql, $expected, 'got expected output from SQL decoding session');

my $endpos = $node_standby->safe_psql('testdb',
	"SELECT lsn FROM pg_logical_slot_peek_changes('activeslot', NULL, NULL) ORDER BY lsn DESC LIMIT 1;"
);
print "waiting to replay $endpos\n";

# Insert some rows after $endpos, which we won't read.
$node_primary->safe_psql('testdb',
	qq[INSERT INTO decoding_test(x,y) SELECT s, s::text FROM generate_series(5,50) s;]
);

$node_primary->wait_for_catchup($node_standby, 'replay', $node_primary->lsn('flush'));

my $stdout_recv = $node_standby->pg_recvlogical_upto(
    'testdb', 'activeslot', $endpos, 180,
    'include-xids'     => '0',
    'skip-empty-xacts' => '1');
chomp($stdout_recv);
is($stdout_recv, $expected,
    'got same expected output from pg_recvlogical decoding session');

$node_standby->poll_query_until('testdb',
	"SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'activeslot' AND active_pid IS NULL)"
) or die "slot never became inactive";

$stdout_recv = $node_standby->pg_recvlogical_upto(
    'testdb', 'activeslot', $endpos, 180,
    'include-xids'     => '0',
    'skip-empty-xacts' => '1');
chomp($stdout_recv);
is($stdout_recv, '', 'pg_recvlogical acknowledged changes');

$node_primary->safe_psql('postgres', 'CREATE DATABASE otherdb');

is( $node_primary->psql(
        'otherdb',
        "SELECT lsn FROM pg_logical_slot_peek_changes('activeslot', NULL, NULL) ORDER BY lsn DESC LIMIT 1;"
    ),
    3,
    'replaying logical slot from another database fails');

# drop the logical slots
$node_standby->psql('postgres', q[SELECT pg_drop_replication_slot('inactiveslot')]);
$node_standby->psql('postgres', q[SELECT pg_drop_replication_slot('activeslot')]);

##################################################
# Recovery conflict: Invalidate conflicting slots, including in-use slots
# Scenario 1: hot_standby_feedback off and vacuum FULL
##################################################

create_logical_slots();

# One way to reproduce recovery conflict is to run VACUUM FULL with
# hot_standby_feedback turned off on the standby.
$node_standby->append_conf('postgresql.conf',q[
hot_standby_feedback = off
]);
$node_standby->restart;
# ensure walreceiver feedback off by waiting for expected xmin and
# catalog_xmin on primary. Both should be NULL since hs_feedback is off
wait_for_xmins($node_primary, $primary_slotname,
			   "xmin IS NULL AND catalog_xmin IS NULL");

$handle = make_slot_active(1);

# This should trigger the conflict
$node_primary->safe_psql('testdb', 'VACUUM FULL');

$node_primary->wait_for_catchup($node_standby, 'replay', $node_primary->lsn('flush'));

# message should be issued
ok( find_in_log(
   $node_standby,
  "invalidating slot \"inactiveslot\" because it conflicts with recovery"),
  'inactiveslot slot invalidation is logged with vacuum FULL');

ok( find_in_log(
   $node_standby,
  "invalidating slot \"activeslot\" because it conflicts with recovery"),
  'activeslot slot invalidation is logged with vacuum FULL');

# Verify that pg_stat_database_conflicts.confl_logicalslot has been updated
ok( $node_standby->poll_query_until(
	'postgres',
	"select (confl_logicalslot = 2) from pg_stat_database_conflicts where datname = 'testdb'", 't'),
	'confl_logicalslot updated') or die "Timed out waiting confl_logicalslot to be updated";

$handle = make_slot_active(0);
usleep(100_000);

# We are not able to read from the slot as it has been invalidated
ok( find_in_log(
   $node_standby,
  "cannot read from logical replication slot \"activeslot\""),
  'cannot read from logical replication slot');

# Turn hot_standby_feedback back on
$node_standby->append_conf('postgresql.conf',q[
hot_standby_feedback = on
]);
$node_standby->restart;

# ensure walreceiver feedback sent by waiting for expected xmin and
# catalog_xmin on primary. With hot_standby_feedback on, xmin should advance,
# but catalog_xmin should still remain NULL since there is no logical slot.
wait_for_xmins($node_primary, $primary_slotname,
			   "xmin IS NOT NULL AND catalog_xmin IS NULL");

##################################################
# Recovery conflict: Invalidate conflicting slots, including in-use slots
# Scenario 2: conflict due to row removal with hot_standby_feedback off.
##################################################

# get the position to search from in the standby logfile
my $logstart = get_log_size($node_standby);

# drop the logical slots
$node_standby->psql('postgres', q[SELECT pg_drop_replication_slot('inactiveslot')]);
$node_standby->psql('postgres', q[SELECT pg_drop_replication_slot('activeslot')]);

create_logical_slots();

# One way to produce recovery conflict is to create/drop a relation and launch a vacuum
# with hot_standby_feedback turned off on the standby.
$node_standby->append_conf('postgresql.conf',q[
hot_standby_feedback = off
]);
$node_standby->restart;
# ensure walreceiver feedback off by waiting for expected xmin and
# catalog_xmin on primary. Both should be NULL since hs_feedback is off
wait_for_xmins($node_primary, $primary_slotname,
			   "xmin IS NULL AND catalog_xmin IS NULL");

$handle = make_slot_active(1);

# This should trigger the conflict
$node_primary->safe_psql('testdb', qq[CREATE TABLE conflict_test(x integer, y text);]);
$node_primary->safe_psql('testdb', qq[DROP TABLE conflict_test;]);
$node_primary->safe_psql('testdb', 'VACUUM');

$node_primary->wait_for_catchup($node_standby, 'replay', $node_primary->lsn('flush'));

# message should be issued
ok( find_in_log(
   $node_standby,
  "invalidating slot \"inactiveslot\" because it conflicts with recovery", $logstart),
  'inactiveslot slot invalidation is logged due to row removal');

ok( find_in_log(
   $node_standby,
  "invalidating slot \"activeslot\" because it conflicts with recovery", $logstart),
  'activeslot slot invalidation is logged due to row removal');

# Verify that pg_stat_database_conflicts.confl_logicalslot has been updated
ok( $node_standby->poll_query_until(
	'postgres',
	"select (confl_logicalslot = 2) from pg_stat_database_conflicts where datname = 'testdb'", 't'),
	'confl_logicalslot updated') or die "Timed out waiting confl_logicalslot to be updated";

$handle = make_slot_active(0);
usleep(100_000);

# We are not able to read from the slot as it has been invalidated
ok( find_in_log(
   $node_standby,
  "cannot read from logical replication slot \"activeslot\"", $logstart),
  'cannot read from logical replication slot');

# Turn hot_standby_feedback back on
$node_standby->append_conf('postgresql.conf',q[
hot_standby_feedback = on
]);
$node_standby->restart;

# ensure walreceiver feedback sent by waiting for expected xmin and
# catalog_xmin on primary. With hot_standby_feedback on, xmin should advance,
# but catalog_xmin should still remain NULL since there is no logical slot.
wait_for_xmins($node_primary, $primary_slotname,
			   "xmin IS NOT NULL AND catalog_xmin IS NULL");

##################################################
# Recovery conflict: Invalidate conflicting slots, including in-use slots
# Scenario 3: incorrect wal_level on primary.
##################################################

# get the position to search from in the standby logfile
$logstart = get_log_size($node_standby);

# drop the logical slots
$node_standby->psql('postgres', q[SELECT pg_drop_replication_slot('inactiveslot')]);
$node_standby->psql('postgres', q[SELECT pg_drop_replication_slot('activeslot')]);

create_logical_slots();

$handle = make_slot_active(1);

# Make primary wal_level replica. This will trigger slot conflict.
$node_primary->append_conf('postgresql.conf',q[
wal_level = 'replica'
]);
$node_primary->restart;

$node_primary->wait_for_catchup($node_standby, 'replay', $node_primary->lsn('flush'));

# message should be issued
ok( find_in_log(
   $node_standby,
  "invalidating slot \"inactiveslot\" because it conflicts with recovery", $logstart),
  'inactiveslot slot invalidation is logged due to wal_level');

ok( find_in_log(
   $node_standby,
  "invalidating slot \"activeslot\" because it conflicts with recovery", $logstart),
  'activeslot slot invalidation is logged due to wal_level');

# Verify that pg_stat_database_conflicts.confl_logicalslot has been updated
ok( $node_standby->poll_query_until(
	'postgres',
	"select (confl_logicalslot = 2) from pg_stat_database_conflicts where datname = 'testdb'", 't'),
	'confl_logicalslot updated') or die "Timed out waiting confl_logicalslot to be updated";

$handle = make_slot_active(0);
usleep(100_000);

ok( find_in_log(
   $node_standby,
  "logical decoding on standby requires wal_level >= logical on master", $logstart),
  'cannot start replication because wal_level < logical on master');

# Restore primary wal_level
$node_primary->append_conf('postgresql.conf',q[
wal_level = 'logical'
]);
$node_primary->restart;
$node_primary->wait_for_catchup($node_standby, 'replay', $node_primary->lsn('flush'));

$handle = make_slot_active(0);
usleep(100_000);

# as the slot has been invalidated we should not be able to read
ok( find_in_log(
   $node_standby,
  "cannot read from logical replication slot \"activeslot\"", $logstart),
  'cannot read from logical replication slot');

##################################################
# DROP DATABASE should drops it's slots, including active slots.
##################################################

$node_standby->psql('postgres', q[SELECT pg_drop_replication_slot('inactiveslot')]);
$node_standby->psql('postgres', q[SELECT pg_drop_replication_slot('activeslot')]);
create_logical_slots();
$handle = make_slot_active(1);
# Create a slot on a database that would not be dropped. This slot should not
# get dropped.
$node_standby->create_logical_slot_on_standby($node_primary, 'otherslot', 'postgres');

# dropdb on the primary to verify slots are dropped on standby
$node_primary->safe_psql('postgres', q[DROP DATABASE testdb]);

$node_primary->wait_for_catchup($node_standby, 'replay', $node_primary->lsn('flush'));

is($node_standby->safe_psql('postgres',
	q[SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = 'testdb')]), 'f',
	'database dropped on standby');

check_slots_dropped($handle);

is($node_standby->slot('otherslot')->{'slot_type'}, 'logical',
	'otherslot on standby not dropped');

# Cleanup : manually drop the slot that was not dropped.
$node_standby->psql('postgres', q[SELECT pg_drop_replication_slot('otherslot')]);

##################################################
# Test standby promotion and logical decoding behavior
# after the standby gets promoted.
##################################################

$node_primary->psql('postgres', q[CREATE DATABASE testdb]);
$node_primary->safe_psql('testdb', qq[CREATE TABLE decoding_test(x integer, y text);]);

# create the logical slots
create_logical_slots();

# Insert some rows before the promotion
$node_primary->safe_psql('testdb',
	qq[INSERT INTO decoding_test(x,y) SELECT s, s::text FROM generate_series(1,4) s;]
);

$node_primary->wait_for_catchup($node_standby, 'replay', $node_primary->lsn('flush'));

# promote
$node_standby->promote;

# insert some rows on promoted standby
$node_standby->safe_psql('testdb',
	qq[INSERT INTO decoding_test(x,y) SELECT s, s::text FROM generate_series(5,7) s;]
);


$expected = q{BEGIN
table public.decoding_test: INSERT: x[integer]:1 y[text]:'1'
table public.decoding_test: INSERT: x[integer]:2 y[text]:'2'
table public.decoding_test: INSERT: x[integer]:3 y[text]:'3'
table public.decoding_test: INSERT: x[integer]:4 y[text]:'4'
COMMIT
BEGIN
table public.decoding_test: INSERT: x[integer]:5 y[text]:'5'
table public.decoding_test: INSERT: x[integer]:6 y[text]:'6'
table public.decoding_test: INSERT: x[integer]:7 y[text]:'7'
COMMIT};

# check that we are decoding pre and post promotion inserted rows
$stdout_sql = $node_standby->safe_psql('testdb',
	qq[SELECT data FROM pg_logical_slot_peek_changes('activeslot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');]
);

is($stdout_sql, $expected, 'got expected output from SQL decoding session on promoted standby');
