# Copyright (c) 2023, PostgreSQL Global Development Group

# Tests for upgrading replication slots

use strict;
use warnings;

use File::Find qw(find);
use File::Path qw(rmtree);

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Can be changed to test the other modes
my $mode = $ENV{PG_TEST_PG_UPGRADE_MODE} || '--copy';

# Initialize old cluster
my $old_publisher = PostgreSQL::Test::Cluster->new('old_publisher');
$old_publisher->init(allows_streaming => 'logical');

# Initialize new cluster
my $new_publisher = PostgreSQL::Test::Cluster->new('new_publisher');
$new_publisher->init(allows_streaming => 'replica');

# Initialize subscriber cluster
my $subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$subscriber->init(allows_streaming => 'logical');

my $bindir = $new_publisher->config_data('--bindir');

# ------------------------------
# TEST: Confirm pg_upgrade fails when new cluster wal_level is not 'logical'

# Preparations for the subsequent test:
# 1. Create a slot on the old cluster
$old_publisher->start;
$old_publisher->safe_psql('postgres',
	"SELECT pg_create_logical_replication_slot('test_slot1', 'test_decoding', false, true);"
);
$old_publisher->stop;

# pg_upgrade will fail because the new cluster wal_level is 'replica'
command_checks_all(
	[
		'pg_upgrade', '--no-sync',
		'-d', $old_publisher->data_dir,
		'-D', $new_publisher->data_dir,
		'-b', $bindir,
		'-B', $bindir,
		'-s', $new_publisher->host,
		'-p', $old_publisher->port,
		'-P', $new_publisher->port,
		$mode,
	],
	1,
	[qr/wal_level must be \"logical\", but is set to \"replica\"/],
	[qr//],
	'run of pg_upgrade where the new cluster has the wrong wal_level');
ok( -d $new_publisher->data_dir . "/pg_upgrade_output.d",
	"pg_upgrade_output.d/ not removed after pg_upgrade failure");

# Clean up
rmtree($new_publisher->data_dir . "/pg_upgrade_output.d");

# ------------------------------
# TEST: Confirm pg_upgrade fails when max_replication_slots on a new cluster is
#		too low

# Preparations for the subsequent test:
# 1. Create a second slot on the old cluster
$old_publisher->start;
$old_publisher->safe_psql('postgres',
	"SELECT pg_create_logical_replication_slot('test_slot2', 'test_decoding', false, true);"
);

# 2. Consume WAL records to avoid another type of upgrade failure. It will be
#	 tested in subsequent cases.
$old_publisher->safe_psql('postgres',
	"SELECT count(*) FROM pg_logical_slot_get_changes('test_slot1', NULL, NULL);"
);
$old_publisher->stop;

# 3. max_replication_slots is set to smaller than the number of slots (2)
#	 present on the old cluster
$new_publisher->append_conf('postgresql.conf', "max_replication_slots = 1");

# 4. wal_level is set correctly on the new cluster
$new_publisher->append_conf('postgresql.conf', "wal_level = 'logical'");

# pg_upgrade will fail because the new cluster has insufficient max_replication_slots
command_checks_all(
	[
		'pg_upgrade', '--no-sync',
		'-d', $old_publisher->data_dir,
		'-D', $new_publisher->data_dir,
		'-b', $bindir,
		'-B', $bindir,
		'-s', $new_publisher->host,
		'-p', $old_publisher->port,
		'-P', $new_publisher->port,
		$mode,
	],
	1,
	[
		qr/max_replication_slots \(1\) must be greater than or equal to the number of logical replication slots \(2\) on the old cluster/
	],
	[qr//],
	'run of pg_upgrade where the new cluster has insufficient max_replication_slots'
);
ok( -d $new_publisher->data_dir . "/pg_upgrade_output.d",
	"pg_upgrade_output.d/ not removed after pg_upgrade failure");

# Clean up
rmtree($new_publisher->data_dir . "/pg_upgrade_output.d");

# ------------------------------
# TEST: Confirm pg_upgrade fails when the slot still has unconsumed WAL records

# Preparations for the subsequent test:
# 1. Remove the slot 'test_slot2', leaving only 1 slot on the old cluster, so
#    the new cluster config  max_replication_slots=1 will now be enough.
$old_publisher->start;
$old_publisher->safe_psql('postgres',
	"SELECT * FROM pg_drop_replication_slot('test_slot2');");

# 2. Generate extra WAL records. Because these WAL records do not get consumed
#	 it will cause the upcoming pg_upgrade test to fail.
$old_publisher->safe_psql('postgres',
	"CREATE TABLE tbl AS SELECT generate_series(1, 10) AS a;");
$old_publisher->stop;

# pg_upgrade will fail because the slot still has unconsumed WAL records
command_checks_all(
	[
		'pg_upgrade', '--no-sync',
		'-d', $old_publisher->data_dir,
		'-D', $new_publisher->data_dir,
		'-b', $bindir,
		'-B', $bindir,
		'-s', $new_publisher->host,
		'-p', $old_publisher->port,
		'-P', $new_publisher->port,
		$mode,
	],
	1,
	[
		qr/Your installation contains logical replication slots that cannot be upgraded./
	],
	[qr//],
	'run of pg_upgrade of old cluster with idle replication slots');
ok( -d $new_publisher->data_dir . "/pg_upgrade_output.d",
	"pg_upgrade_output.d/ not removed after pg_upgrade failure");

# Verify the reason why the logical replication slot cannot be upgraded
my $log_path = $new_publisher->data_dir . "/pg_upgrade_output.d";
my $slots_filename;

# Find a txt file that contains a list of logical replication slots that cannot
# be upgraded. We cannot predict the file's path because the output directory
# contains a milliseconds timestamp. File::Find::find must be used.
find(
	sub {
		if ($File::Find::name =~ m/invalid_logical_relication_slots\.txt/)
		{
			$slots_filename = $File::Find::name;
		}
	},
	$new_publisher->data_dir . "/pg_upgrade_output.d");

# And check the content. The failure should be because there are unconsumed
# WALs after confirmed_flush_lsn of test_slot1.
like(
	slurp_file($slots_filename),
	qr/The slot \"test_slot1\" has not consumed the WAL yet/m,
	'the previous test failed due to unconsumed WALs');

# Clean up
rmtree($new_publisher->data_dir . "/pg_upgrade_output.d");
# Remove the remaining slot
$old_publisher->start;
$old_publisher->safe_psql('postgres',
	"SELECT * FROM pg_drop_replication_slot('test_slot1');");

# ------------------------------
# TEST: Successful upgrade

# Preparations for the subsequent test:
# 1. Setup logical replication
my $old_connstr = $old_publisher->connstr . ' dbname=postgres';
$old_publisher->safe_psql('postgres',
	"CREATE PUBLICATION pub FOR ALL TABLES;");
$subscriber->start;
$subscriber->safe_psql(
	'postgres', qq[
	CREATE TABLE tbl (a int);
	CREATE SUBSCRIPTION regress_sub CONNECTION '$old_connstr' PUBLICATION pub WITH (two_phase = 'true')
]);
$subscriber->wait_for_subscription_sync($old_publisher, 'regress_sub');

# 2. Temporarily disable the subscription
$subscriber->safe_psql('postgres', "ALTER SUBSCRIPTION regress_sub DISABLE");
$old_publisher->stop;

# Dry run, successful check is expected. This is not a live check, so a
# shutdown checkpoint record would be inserted. We want to test that a
# subsequent upgrade is successful by skipping such an expected WAL record.
command_ok(
	[
		'pg_upgrade', '--no-sync',
		'-d', $old_publisher->data_dir,
		'-D', $new_publisher->data_dir,
		'-b', $bindir,
		'-B', $bindir,
		'-s', $new_publisher->host,
		'-p', $old_publisher->port,
		'-P', $new_publisher->port,
		$mode, '--check'
	],
	'run of pg_upgrade of old cluster');
ok( !-d $new_publisher->data_dir . "/pg_upgrade_output.d",
	"pg_upgrade_output.d/ removed after pg_upgrade success");

# Actual run, successful upgrade is expected
command_ok(
	[
		'pg_upgrade', '--no-sync',
		'-d', $old_publisher->data_dir,
		'-D', $new_publisher->data_dir,
		'-b', $bindir,
		'-B', $bindir,
		'-s', $new_publisher->host,
		'-p', $old_publisher->port,
		'-P', $new_publisher->port,
		$mode,
	],
	'run of pg_upgrade of old cluster');
ok( !-d $new_publisher->data_dir . "/pg_upgrade_output.d",
	"pg_upgrade_output.d/ removed after pg_upgrade success");

# Check that the slot 'regress_sub' has migrated to the new cluster
$new_publisher->start;
my $result = $new_publisher->safe_psql('postgres',
	"SELECT slot_name, two_phase FROM pg_replication_slots");
is($result, qq(regress_sub|t), 'check the slot exists on new cluster');

# Update the connection
my $new_connstr = $new_publisher->connstr . ' dbname=postgres';
$subscriber->safe_psql(
	'postgres', qq[
	ALTER SUBSCRIPTION regress_sub CONNECTION '$new_connstr';
	ALTER SUBSCRIPTION regress_sub ENABLE;
]);

# Check whether changes on the new publisher get replicated to the subscriber
$new_publisher->safe_psql('postgres',
	"INSERT INTO tbl VALUES (generate_series(11, 20))");
$new_publisher->wait_for_catchup('regress_sub');
$result = $subscriber->safe_psql('postgres', "SELECT count(*) FROM tbl");
is($result, qq(20), 'check changes are replicated to the subscriber');

# Clean up
$subscriber->stop();
$new_publisher->stop();

done_testing();
