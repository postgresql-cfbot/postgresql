# Copyright (c) 2022-2023, PostgreSQL Global Development Group

# Test for pg_upgrade of logical subscription
use strict;
use warnings;

use Cwd qw(abs_path);
use File::Basename qw(dirname);
use File::Compare;
use File::Find qw(find);
use File::Path qw(rmtree);

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::AdjustUpgrade;
use Test::More;

# Can be changed to test the other modes.
my $mode = $ENV{PG_TEST_PG_UPGRADE_MODE} || '--copy';

# Initialize publisher node
my $publisher = PostgreSQL::Test::Cluster->new('publisher');
$publisher->init(allows_streaming => 'logical');
$publisher->start;

# Initialize the old subscriber node
my $old_sub = PostgreSQL::Test::Cluster->new('old_sub');
$old_sub->init;
$old_sub->start;

# Initialize the new subscriber
my $new_sub = PostgreSQL::Test::Cluster->new('new_sub');
$new_sub->init;
my $bindir = $new_sub->config_data('--bindir');

sub insert_line
{
	my $payload = shift;

	foreach("t1", "t2")
	{
		$publisher->safe_psql('postgres',
			"INSERT INTO " . $_ . " (val) VALUES('$payload')");
	}
}

# Initial setup
foreach ("t1", "t2")
{
	$publisher->safe_psql('postgres',
		"CREATE TABLE " . $_ . " (id serial, val text)");
	$old_sub->safe_psql('postgres',
		"CREATE TABLE " . $_ . " (id serial, val text)");
}
insert_line('before initial sync');

# Setup logical replication, replicating only 1 table
my $connstr = $publisher->connstr . ' dbname=postgres';

$publisher->safe_psql('postgres',
	"CREATE PUBLICATION pub FOR TABLE t1");

$old_sub->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub CONNECTION '$connstr' PUBLICATION pub");

# Wait for the catchup, as we need the subscription rel in ready state
$old_sub->wait_for_subscription_sync($publisher, 'sub');

# replication origin's remote_lsn isn't set if not data is replicated after the
# initial sync
command_fails(
	[
		'pg_upgrade', '--no-sync',        '-d', $old_sub->data_dir,
		'-D',         $new_sub->data_dir, '-b', $bindir,
		'-B',         $bindir,            '-s', $new_sub->host,
		'-p',         $old_sub->port,     '-P', $new_sub->port,
		$mode,
		'--preserve-subscription-state',
		'--check',
	],
	'run of pg_upgrade --check for old instance with invalid remote_lsn');
ok(-d $new_sub->data_dir . "/pg_upgrade_output.d",
	"pg_upgrade_output.d/ not removed after pg_upgrade failure");
rmtree($new_sub->data_dir . "/pg_upgrade_output.d");

# Make sure the replication origin is set
insert_line('after initial sync');
$publisher->wait_for_catchup('sub');

my $result = $old_sub->safe_psql('postgres',
    "SELECT COUNT(*) FROM pg_subscription_rel WHERE srsubstate != 'r'");
is ($result, qq(0), "All tables in pg_subscription_rel should be in ready state");

# Check the number of rows for each table on each server
$result = $publisher->safe_psql('postgres',
	"SELECT count(*) FROM t1");
is ($result, qq(2), "Table t1 should have 2 rows on the publisher");
$result = $publisher->safe_psql('postgres',
	"SELECT count(*) FROM t2");
is ($result, qq(2), "Table t2 should have 2 rows on the publisher");
$result = $old_sub->safe_psql('postgres',
	"SELECT count(*) FROM t1");
is ($result, qq(2), "Table t1 should have 2 rows on the old subscriber");
$result = $old_sub->safe_psql('postgres',
	"SELECT count(*) FROM t2");
is ($result, qq(0), "Table t2 should have 0 rows on the old subscriber");

# Check that pg_upgrade refuses to upgrade subscription with non ready tables
$old_sub->safe_psql('postgres',
    "ALTER SUBSCRIPTION sub DISABLE");
$old_sub->safe_psql('postgres',
	"UPDATE pg_subscription_rel
		SET srsubstate = 'i' WHERE srsubstate = 'r'");

command_fails(
	[
		'pg_upgrade', '--no-sync',        '-d', $old_sub->data_dir,
		'-D',         $new_sub->data_dir, '-b', $bindir,
		'-B',         $bindir,            '-s', $new_sub->host,
		'-p',         $old_sub->port,     '-P', $new_sub->port,
		$mode,
		'--preserve-subscription-state',
		'--check',
	],
	'run of pg_upgrade --check for old instance with incorrect sub rel');
ok(-d $new_sub->data_dir . "/pg_upgrade_output.d",
	"pg_upgrade_output.d/ not removed after pg_upgrade failure");
rmtree($new_sub->data_dir . "/pg_upgrade_output.d");

# and otherwise works
$old_sub->safe_psql('postgres',
	"UPDATE pg_subscription_rel
		SET srsubstate = 'r' WHERE srsubstate = 'i'");

command_ok(
	[
		'pg_upgrade', '--no-sync',        '-d', $old_sub->data_dir,
		'-D',         $new_sub->data_dir, '-b', $bindir,
		'-B',         $bindir,            '-s', $new_sub->host,
		'-p',         $old_sub->port,     '-P', $new_sub->port,
		$mode,
		'--preserve-subscription-state',
		'--check',
	],
	'run of pg_upgrade --check for old instance with correct sub rel');

# Stop the old subscriber, insert a row in each table while it's down and add
# t2 to the publication
$old_sub->stop;

insert_line('while old_sub is down');

# Run pg_upgrade
command_ok(
	[
		'pg_upgrade', '--no-sync',        '-d', $old_sub->data_dir,
		'-D',         $new_sub->data_dir, '-b', $bindir,
		'-B',         $bindir,            '-s', $new_sub->host,
		'-p',         $old_sub->port,     '-P', $new_sub->port,
		$mode,
		'--preserve-subscription-state',
	],
	'run of pg_upgrade for new sub');
ok( !-d $new_sub->data_dir . "/pg_upgrade_output.d",
	"pg_upgrade_output.d/ removed after pg_upgrade success");
$publisher->safe_psql('postgres',
	"ALTER PUBLICATION pub ADD TABLE t2");

$new_sub->start;

# There should be no new replicated rows before enabling the subscription
$result = $new_sub->safe_psql('postgres',
	"SELECT count(*) FROM t1");
is ($result, qq(2), "Table t1 should still have 2 rows on the new subscriber");
$result = $new_sub->safe_psql('postgres',
	"SELECT count(*) FROM t2");
is ($result, qq(0), "Table t2 should still have 0 rows on the new subscriber");

# Enable the subscription
$new_sub->safe_psql('postgres',
	"ALTER SUBSCRIPTION sub ENABLE");

$publisher->wait_for_catchup('sub');

# Rows on t1 should have been replicated
$result = $new_sub->safe_psql('postgres',
	"SELECT count(*) FROM t1");
is ($result, qq(3), "Table t1 should now have 3 rows on the new subscriber");
$result = $new_sub->safe_psql('postgres',
	"SELECT count(*) FROM t2");
is ($result, qq(0), "Table t2 should still have 0 rows on the new subscriber");

# Refresh the subscription, only the missing row on t2 show be replicated
$new_sub->safe_psql('postgres',
	"ALTER SUBSCRIPTION sub REFRESH PUBLICATION");
$publisher->wait_for_catchup('sub');
$result = $new_sub->safe_psql('postgres',
	"SELECT count(*) FROM t1");
is ($result, qq(3), "Table t1 should still have 3 rows on the new subscriber");
$result = $new_sub->safe_psql('postgres',
	"SELECT count(*) FROM t2");
is ($result, qq(3), "Table t2 should now have 3 rows on the new subscriber");

done_testing();
