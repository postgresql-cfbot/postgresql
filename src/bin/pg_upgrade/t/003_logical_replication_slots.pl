# Copyright (c) 2023, PostgreSQL Global Development Group

# Tests for upgrading replication slots

use strict;
use warnings;

use File::Path qw(rmtree);

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Can be changed to test the other modes
my $mode = $ENV{PG_TEST_PG_UPGRADE_MODE} || '--copy';

# Initialize old node
my $old_publisher = PostgreSQL::Test::Cluster->new('old_publisher');
$old_publisher->init(allows_streaming => 'logical');

# Initialize new node
my $new_publisher = PostgreSQL::Test::Cluster->new('new_publisher');
$new_publisher->init(allows_streaming => 1);

# Initialize subscriber node
my $subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$subscriber->init(allows_streaming => 'logical');

my $bindir = $new_publisher->config_data('--bindir');

# Cause a failure at the start of pg_upgrade because wal_level is replica
command_fails(
	[
		'pg_upgrade', '--no-sync',
		'-d',         $old_publisher->data_dir,
		'-D',         $new_publisher->data_dir,
		'-b',         $bindir,
		'-B',         $bindir,
		'-s',         $new_publisher->host,
		'-p',         $old_publisher->port,
		'-P',         $new_publisher->port,
		$mode,        '--include-logical-replication-slots',
	],
	'run of pg_upgrade of old node with wrong wal_level');
ok( -d $new_publisher->data_dir . "/pg_upgrade_output.d",
	"pg_upgrade_output.d/ not removed after pg_upgrade failure");

# Clean up
rmtree($new_publisher->data_dir . "/pg_upgrade_output.d");

# Preparations for the subsequent test. The case max_replication_slots is set
# to 0 is prohibited.
$new_publisher->append_conf('postgresql.conf', "wal_level = 'logical'");
$new_publisher->append_conf('postgresql.conf', "max_replication_slots = 0");

# Cause a failure at the start of pg_upgrade because max_replication_slots is 0
command_fails(
	[
		'pg_upgrade', '--no-sync',
		'-d',         $old_publisher->data_dir,
		'-D',         $new_publisher->data_dir,
		'-b',         $bindir,
		'-B',         $bindir,
		'-s',         $new_publisher->host,
		'-p',         $old_publisher->port,
		'-P',         $new_publisher->port,
		$mode,        '--include-logical-replication-slots',
	],
	'run of pg_upgrade of old node with wrong max_replication_slots');
ok( -d $new_publisher->data_dir . "/pg_upgrade_output.d",
	"pg_upgrade_output.d/ not removed after pg_upgrade failure");

# Clean up
rmtree($new_publisher->data_dir . "/pg_upgrade_output.d");

# Preparations for the subsequent test. max_replication_slots is set to
# non-zero value
$new_publisher->append_conf('postgresql.conf', "max_replication_slots = 1");

# Create a slot on old node
$old_publisher->start;
$old_publisher->safe_psql(
	'postgres', qq[
	SELECT pg_create_logical_replication_slot('test_slot1', 'test_decoding', false, true);
	SELECT pg_create_logical_replication_slot('test_slot2', 'test_decoding', false, true);
	CREATE TABLE tbl AS SELECT generate_series(1, 10) AS a;
]);

$old_publisher->stop;

# Cause a failure at the start of pg_upgrade because max_replication_slots is
# smaller than existing slots on old node
command_fails(
	[
		'pg_upgrade', '--no-sync',
		'-d',         $old_publisher->data_dir,
		'-D',         $new_publisher->data_dir,
		'-b',         $bindir,
		'-B',         $bindir,
		'-s',         $new_publisher->host,
		'-p',         $old_publisher->port,
		'-P',         $new_publisher->port,
		$mode,        '--include-logical-replication-slots',
	],
	'run of pg_upgrade of old node with small max_replication_slots');
ok( -d $new_publisher->data_dir . "/pg_upgrade_output.d",
	"pg_upgrade_output.d/ not removed after pg_upgrade failure");

# Clean up
rmtree($new_publisher->data_dir . "/pg_upgrade_output.d");

# Preparations for the subsequent test. max_replication_slots is set to
# appropriate value
$new_publisher->append_conf('postgresql.conf', "max_replication_slots = 10");

$old_publisher->start;
$old_publisher->safe_psql(
	'postgres', qq[
	SELECT pg_drop_replication_slot('test_slot1');
	SELECT pg_drop_replication_slot('test_slot2');
]);

# Setup logical replication
my $old_connstr = $old_publisher->connstr . ' dbname=postgres';
$old_publisher->safe_psql('postgres', "CREATE PUBLICATION pub FOR ALL TABLES");
$subscriber->start;
$subscriber->safe_psql(
	'postgres', qq[
	CREATE TABLE tbl (a int);
	CREATE SUBSCRIPTION sub CONNECTION '$old_connstr' PUBLICATION pub WITH (two_phase = 'true')
]);

# Wait for initial table sync to finish
$subscriber->wait_for_subscription_sync($old_publisher, 'sub');

$subscriber->safe_psql('postgres', "ALTER SUBSCRIPTION sub DISABLE");
$old_publisher->stop;

# Actual run, pg_upgrade_output.d is removed at the end
command_ok(
	[
		'pg_upgrade', '--no-sync',
		'-d',         $old_publisher->data_dir,
		'-D',         $new_publisher->data_dir,
		'-b',         $bindir,
		'-B',         $bindir,
		'-s',         $new_publisher->host,
		'-p',         $old_publisher->port,
		'-P',         $new_publisher->port,
		$mode,        '--include-logical-replication-slots'
	],
	'run of pg_upgrade of old node');
ok( !-d $new_publisher->data_dir . "/pg_upgrade_output.d",
	"pg_upgrade_output.d/ removed after pg_upgrade success");

$new_publisher->start;
my $result = $new_publisher->safe_psql('postgres',
	"SELECT slot_name, two_phase FROM pg_replication_slots");
is($result, qq(sub|t), 'check the slot exists on new node');

# Change the connection
my $new_connstr = $new_publisher->connstr . ' dbname=postgres';
$subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION sub CONNECTION '$new_connstr'");
$subscriber->safe_psql('postgres', "ALTER SUBSCRIPTION sub ENABLE");

# Check whether changes on the new publisher get replicated to the subscriber
$new_publisher->safe_psql('postgres',
	"INSERT INTO tbl VALUES (generate_series(11, 20))");
$new_publisher->wait_for_catchup('sub');
$result = $subscriber->safe_psql('postgres', "SELECT count(*) FROM tbl");
is($result, qq(20), 'check changes are shipped to subscriber');

done_testing();
