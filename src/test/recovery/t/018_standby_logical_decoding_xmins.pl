# logical decoding on a standby : ensure xmins are appropriately updated

use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 27;
use RecursiveCopy;
use File::Copy;
use Time::HiRes qw(usleep);

my ($stdin, $stdout, $stderr, $ret, $handle, $slot);

my $node_master = get_new_node('master');
my $node_standby = get_new_node('standby');

# Name for the physical slot on master
my $master_slotname = 'master_physical';
# Name for the logical slot on standby
my $standby_slotname = 'standby_logical';

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

$node_master->safe_psql('postgres', qq[SELECT * FROM pg_create_physical_replication_slot('$master_slotname');]);
my $backup_name = 'b1';
$node_master->backup($backup_name);

# After slot creation, xmins must be null
$slot = $node_master->slot($master_slotname);
is($slot->{'xmin'}, '', "xmin null");
is($slot->{'catalog_xmin'}, '', "catalog_xmin null");

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


################################
# xmin/catalog_xmin verification before and after standby-logical-slot creation.
################################

# With hot_standby_feedback off, xmin and catalog_xmin must still be null
$slot = $node_master->slot($master_slotname);
is($slot->{'xmin'}, '', "xmin null after standby join");
is($slot->{'catalog_xmin'}, '', "catalog_xmin null after standby join");

$node_standby->append_conf('postgresql.conf',q[
hot_standby_feedback = on
]);
$node_standby->restart;

# ensure walreceiver feedback sent by waiting for expected xmin and
# catalog_xmin on master. With hot_standby_feedback on, xmin should advance,
# but catalog_xmin should still remain NULL since there is no logical slot.
wait_for_xmins($node_master, $master_slotname, "xmin IS NOT NULL AND catalog_xmin IS NULL");

# Create new slots on the standby, ignoring the ones on the master completely.
#
# This must succeed since we know we have a catalog_xmin reservation. We
# might've already sent hot standby feedback to advance our physical slot's
# catalog_xmin but not received the corresponding xlog for the catalog xmin
# advance, in which case we'll create a slot that isn't usable. The calling
# application can prevent this by creating a temporary slot on the master to
# lock in its catalog_xmin. For a truly race-free solution we'd need
# master-to-standby hot_standby_feedback replies.
#
# In this case it won't race because there's no concurrent activity on the
# master.
#
$node_standby->create_logical_slot_on_standby($node_master, $standby_slotname, 'postgres');

$node_master->wait_for_catchup($node_standby, 'replay', $node_master->lsn('flush'));

# Now that slot is created on standby, catalog_xmin should be non NULL on both
# master and standby.
$slot = $node_master->slot($master_slotname);
isnt($slot->{'xmin'}, '', "physical xmin not null");
isnt($slot->{'catalog_xmin'}, '', "physical catalog_xmin not null");

$slot = $node_standby->slot($standby_slotname);
is($slot->{'xmin'}, '', "logical xmin null");
isnt($slot->{'catalog_xmin'}, '', "logical catalog_xmin not null");


################################
# Standby logical slot should be able to fetch the table changes even when the
# table is dropped.
################################

$node_master->safe_psql('postgres', 'CREATE TABLE test_table(id serial primary key, blah text)');
$node_master->safe_psql('postgres', q[INSERT INTO test_table(blah) values ('itworks')]);
$node_master->safe_psql('postgres', 'DROP TABLE test_table');
$node_master->safe_psql('postgres', 'VACUUM');

$node_master->wait_for_catchup($node_standby, 'replay', $node_master->lsn('flush'));

$slot = $node_master->slot($master_slotname);
isnt($slot->{'xmin'}, '', "physical xmin not null");
isnt($slot->{'catalog_xmin'}, '', "physical catalog_xmin not null");

$node_master->wait_for_catchup($node_standby, 'replay', $node_master->lsn('flush'));

# Should show the inserts even when the table is dropped on master
($ret, $stdout, $stderr) = $node_standby->psql('postgres', qq[SELECT data FROM pg_logical_slot_get_changes('$standby_slotname', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1', 'include-timestamp', '0')]);
is($stderr, '', 'stderr is empty');
is($ret, 0, 'replay from slot succeeded')
	or die 'cannot continue if slot replay fails';
is($stdout, q{BEGIN
table public.test_table: INSERT: id[integer]:1 blah[text]:'itworks'
COMMIT}, 'replay results match');

$node_master->wait_for_catchup($node_standby, 'replay', $node_master->lsn('flush'));

$slot = $node_master->slot($master_slotname);
isnt($slot->{'xmin'}, '', "physical xmin not null");
my $saved_physical_catalog_xmin = $slot->{'catalog_xmin'};
isnt($saved_physical_catalog_xmin, '', "physical catalog_xmin not null");

$slot = $node_standby->slot($standby_slotname);
is($slot->{'xmin'}, '', "logical xmin null");
my $saved_logical_catalog_xmin = $slot->{'catalog_xmin'};
isnt($saved_logical_catalog_xmin, '', "logical catalog_xmin not null");


################################
# Catalog xmins should advance after standby logical slot fetches the changes.
################################

# Ideally we'd just hold catalog_xmin, but since hs_feedback currently uses the slot,
# we hold down xmin.
$node_master->safe_psql('postgres', qq[CREATE TABLE catalog_increase_1();]);
$node_master->safe_psql('postgres', 'CREATE TABLE test_table(id serial primary key, blah text)');
for my $i (0 .. 2000)
{
    $node_master->safe_psql('postgres', qq[INSERT INTO test_table(blah) VALUES ('entry $i')]);
}
$node_master->safe_psql('postgres', qq[CREATE TABLE catalog_increase_2();]);
$node_master->safe_psql('postgres', 'VACUUM');

cmp_ok($node_standby->slot($standby_slotname)->{'catalog_xmin'}, "==",
	   $saved_logical_catalog_xmin,
	   "logical slot catalog_xmin hasn't advanced before get_changes");

($ret, $stdout, $stderr) = $node_standby->psql('postgres',
	qq[SELECT data FROM pg_logical_slot_get_changes('$standby_slotname', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1', 'include-timestamp', '0')]);
is($ret, 0, 'replay of big series succeeded');
isnt($stdout, '', 'replayed some rows');

$node_master->wait_for_catchup($node_standby, 'replay', $node_master->lsn('flush'));

# logical slot catalog_xmin on slave should advance after pg_logical_slot_get_changes
wait_for_xmins($node_standby, $standby_slotname,
			   "catalog_xmin::varchar::int > ${saved_logical_catalog_xmin}");
$slot = $node_standby->slot($standby_slotname);
my $new_logical_catalog_xmin = $slot->{'catalog_xmin'};
is($slot->{'xmin'}, '', "logical xmin null");

# hot standby feedback should advance master's phys catalog_xmin now that the
# standby's slot doesn't hold it down as far.
wait_for_xmins($node_master, $master_slotname,
			   "catalog_xmin::varchar::int > ${saved_physical_catalog_xmin}");
$slot = $node_master->slot($master_slotname);
isnt($slot->{'xmin'}, '', "physical xmin not null");

# But master's phys catalog_xmin should not go past the slave's logical slot's
# catalog_xmin
cmp_ok($slot->{'catalog_xmin'}, "<=", $new_logical_catalog_xmin,
	'upstream physical slot catalog_xmin not past downstream catalog_xmin with hs_feedback on');


######################
# Upstream oldestXid should not go past downstream catalog_xmin
######################

# First burn some xids on the master in another DB, so we push the master's
# nextXid ahead.
foreach my $i (1 .. 100)
{
	$node_master->safe_psql('postgres', 'SELECT txid_current()');
}

# Force vacuum freeze on the master and ensure its oldestXmin doesn't advance
# past our needed xmin. The only way we have visibility into that is to force
# a checkpoint.
$node_master->safe_psql('postgres', "UPDATE pg_database SET datallowconn = true WHERE datname = 'template0'");
foreach my $dbname ('template1', 'postgres', 'postgres', 'template0')
{
	$node_master->safe_psql($dbname, 'VACUUM FREEZE');
}
$node_master->safe_psql('postgres', 'CHECKPOINT');
IPC::Run::run(['pg_controldata', $node_master->data_dir()], '>', \$stdout)
	or die "pg_controldata failed with $?";
my @checkpoint = split('\n', $stdout);
my $oldestXid = '';
foreach my $line (@checkpoint)
{
	if ($line =~ qr/^Latest checkpoint's oldestXID:\s+(\d+)/)
	{
		$oldestXid = $1;
	}
}
die 'no oldestXID found in checkpoint' unless $oldestXid;

cmp_ok($oldestXid, "<=", $node_standby->slot($standby_slotname)->{'catalog_xmin'},
	   'upstream oldestXid not past downstream catalog_xmin with hs_feedback on');

$node_master->safe_psql('postgres',
	"UPDATE pg_database SET datallowconn = false WHERE datname = 'template0'");


##################################################
# Drop slot
# Make sure standby slots are droppable, and properly clear the upstream's xmin
##################################################

is($node_standby->safe_psql('postgres', 'SHOW hot_standby_feedback'), 'on', 'hs_feedback is on');

$node_standby->psql('postgres', qq[SELECT pg_drop_replication_slot('$standby_slotname')]);

is($node_standby->slot($standby_slotname)->{'slot_type'}, '', 'slot on standby dropped manually');

# ensure walreceiver feedback sent by waiting for expected xmin and
# catalog_xmin on master. catalog_xmin should become NULL because we dropped
# the logical slot.
wait_for_xmins($node_master, $master_slotname,
			   "xmin IS NOT NULL AND catalog_xmin IS NULL");
