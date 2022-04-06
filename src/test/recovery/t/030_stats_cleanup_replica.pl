# Copyright (c) 2021-2022, PostgreSQL Global Development Group

# Tests that statistics are removed from a physical replica after being dropped
# on the primary

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Initialize primary node
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
# A specific role is created to perform some tests related to replication,
# and it needs proper authentication configuration.
$node_primary->init(allows_streaming => 1);

# Set track_functions to all on primary
$node_primary->append_conf('postgresql.conf', "track_functions = 'all'");
$node_primary->start;

# Take backup
my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

# Create streaming standby linking to primary
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_standby->start;

my $sect = 'initial';

sub populate_standby_stats
{
	my ($connect_db, $schema) = @_;
	# Create table on primary
	$node_primary->safe_psql($connect_db,
		"CREATE TABLE $schema.drop_tab_test1 AS SELECT generate_series(1,100) AS a"
	);

	# Create function on primary
	$node_primary->safe_psql($connect_db,
		"CREATE FUNCTION $schema.drop_func_test1() RETURNS VOID AS 'select 2;' LANGUAGE SQL IMMUTABLE"
	);

	# Wait for catchup
	my $primary_lsn = $node_primary->lsn('write');
	$node_primary->wait_for_catchup($node_standby, 'replay', $primary_lsn);

	# Get database oid
	my $dboid = $node_standby->safe_psql($connect_db,
		"SELECT oid FROM pg_database WHERE datname = '$connect_db'");

	# Get table oid
	my $tableoid = $node_standby->safe_psql($connect_db,
		"SELECT '$schema.drop_tab_test1'::regclass::oid");

	# Do scan on standby
	$node_standby->safe_psql($connect_db,
		"SELECT * FROM $schema.drop_tab_test1");

	# Get function oid
	my $funcoid = $node_standby->safe_psql($connect_db,
		"SELECT '$schema.drop_func_test1()'::regprocedure::oid");

	# Call function on standby
	$node_standby->safe_psql($connect_db, "SELECT $schema.drop_func_test1()");

	return ($dboid, $tableoid, $funcoid);
}

sub drop_function_by_oid
{
	my ($connect_db, $funcoid) = @_;

	# Get function name from returned oid
	my $func_name = $node_primary->safe_psql($connect_db,
		"SELECT '$funcoid'::regprocedure");

	# Drop function on primary
	$node_primary->safe_psql($connect_db, "DROP FUNCTION $func_name");
}

sub drop_table_by_oid
{
	my ($connect_db, $tableoid) = @_;

	# Get table name from returned oid
	my $table_name =
	  $node_primary->safe_psql($connect_db, "SELECT '$tableoid'::regclass");

	# Drop table on primary
	$node_primary->safe_psql($connect_db, "DROP TABLE $table_name");
}

sub test_standby_func_tab_stats_status
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;
	my ($connect_db, $dboid, $tableoid, $funcoid, $present) = @_;

	my %expected = (rel => $present, func => $present);
	my %stats;

	$stats{rel} = $node_standby->safe_psql($connect_db,
		"SELECT pg_stat_exists_stat('relation', $dboid, $tableoid)");
	$stats{func} = $node_standby->safe_psql($connect_db,
		"SELECT pg_stat_exists_stat('function', $dboid, $funcoid)");

	is_deeply(\%stats, \%expected, "$sect: standby stats as expected");

	return;
}

sub test_standby_db_stats_status
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;
	my ($connect_db, $dboid, $present) = @_;

	is( $node_standby->safe_psql(
			$connect_db, "SELECT pg_stat_exists_stat('database', $dboid, 0)"),
		$present,
		"$sect: standby db stats as expected");
}

# Test that stats are cleaned up on standby after dropping table or function

# Populate test objects
my ($dboid, $tableoid, $funcoid) =
  populate_standby_stats('postgres', 'public');

# Test that the stats are present
test_standby_func_tab_stats_status('postgres',
	$dboid, $tableoid, $funcoid, 't');

# Drop test objects
drop_table_by_oid('postgres', $tableoid);

drop_function_by_oid('postgres', $funcoid);

$sect = 'post drop';

# Wait for catchup
my $primary_lsn = $node_primary->lsn('write');
$node_primary->wait_for_catchup($node_standby, 'replay', $primary_lsn);

# Check table and function stats removed from standby
test_standby_func_tab_stats_status('postgres',
	$dboid, $tableoid, $funcoid, 'f');

# Check that stats are cleaned up on standby after dropping schema

$sect = "schema creation";

# Create schema
$node_primary->safe_psql('postgres', "CREATE SCHEMA drop_schema_test1");

# Wait for catchup
$primary_lsn = $node_primary->lsn('write');
$node_primary->wait_for_catchup($node_standby, 'replay', $primary_lsn);

# Populate test objects
($dboid, $tableoid, $funcoid) =
  populate_standby_stats('postgres', 'drop_schema_test1');

# Test that the stats are present
test_standby_func_tab_stats_status('postgres',
	$dboid, $tableoid, $funcoid, 't');

# Drop schema
$node_primary->safe_psql('postgres', "DROP SCHEMA drop_schema_test1 CASCADE");

$sect = "post schema drop";

# Wait for catchup
$primary_lsn = $node_primary->lsn('write');
$node_primary->wait_for_catchup($node_standby, 'replay', $primary_lsn);

# Check table and function stats removed from standby
test_standby_func_tab_stats_status('postgres',
	$dboid, $tableoid, $funcoid, 'f');

# Test that stats are cleaned up on standby after dropping database

# Create the database
$node_primary->safe_psql('postgres', "CREATE DATABASE test");

$sect = "createdb";

# Wait for catchup
$primary_lsn = $node_primary->lsn('write');
$node_primary->wait_for_catchup($node_standby, 'replay', $primary_lsn);

# Populate test objects
($dboid, $tableoid, $funcoid) = populate_standby_stats('test', 'public');

# Test that the stats are present
test_standby_func_tab_stats_status('test', $dboid, $tableoid, $funcoid, 't');

test_standby_db_stats_status('test', $dboid, 't');

# Drop db 'test' on primary
$node_primary->safe_psql('postgres', "DROP DATABASE test");
$sect = "post dropdb";

# Wait for catchup
$primary_lsn = $node_primary->lsn('write');
$node_primary->wait_for_catchup($node_standby, 'replay', $primary_lsn);

# Test that the stats were cleaned up on standby
# Note that this connects to 'postgres' but provides the dboid of dropped db
# 'test' which was returned by previous routine
test_standby_func_tab_stats_status('postgres',
	$dboid, $tableoid, $funcoid, 'f');

test_standby_db_stats_status('postgres', $dboid, 'f');

# verify that stats persist across graceful restarts on a replica

# NB: Can't test database stats, they're immediately repopulated when
# reconnecting...
$sect = "pre restart";
($dboid, $tableoid, $funcoid) = populate_standby_stats('postgres', 'public');
test_standby_func_tab_stats_status('postgres',
	$dboid, $tableoid, $funcoid, 't');

$node_standby->restart();

$sect = "post non-immediate";

test_standby_func_tab_stats_status('postgres',
	$dboid, $tableoid, $funcoid, 't');

# but gone after an immediate restart
$node_standby->stop('immediate');
$node_standby->start();

$sect = "post immediate restart";

test_standby_func_tab_stats_status('postgres',
	$dboid, $tableoid, $funcoid, 'f');

done_testing();
