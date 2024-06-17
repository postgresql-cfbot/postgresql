# Copyright (c) 2024, PostgreSQL Global Development Group

# Additional tests for altering two_phase option
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Initialize publisher node
my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf('postgresql.conf',
	qq(max_prepared_transactions = 10));
$node_publisher->start;

# Create subscriber node
my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init;
$node_subscriber->append_conf('postgresql.conf',
	qq(max_prepared_transactions = 10
	log_min_messages = debug1));
$node_subscriber->start;

# Define tables on both nodes
$node_publisher->safe_psql('postgres',
    "CREATE TABLE tab_full (a int PRIMARY KEY);");
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab_full (a int PRIMARY KEY)");

# Setup logical replication, with two_phase = "false"
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION pub FOR ALL TABLES");

my $log_offset = -s $node_subscriber->logfile;

$node_subscriber->safe_psql(
	'postgres', "
	CREATE SUBSCRIPTION regress_sub
	CONNECTION '$publisher_connstr' PUBLICATION pub
	WITH (two_phase = false, copy_data = false, failover = false)");

# Verify the started worker recognized two_phase was disabled
$node_subscriber->wait_for_log(
	'logical replication apply worker for subscription "regress_sub" two_phase is DISABLED', $log_offset);

#####################
# Check the case that prepared transactions exist on the publisher node.
#
# Since the two_phase is "false", then normally, this PREPARE will do nothing
# until the COMMIT PREPARED, but in this test, we toggle the two_phase to
# "true" again before the COMMIT PREPARED happens.

# Prepare a transaction to insert some tuples into the table
$node_publisher->safe_psql(
	'postgres', "
	BEGIN;
	INSERT INTO tab_full VALUES (generate_series(1, 5));
	PREPARE TRANSACTION 'test_prepared_tab_full';");

$node_publisher->wait_for_catchup('regress_sub');

# Verify the prepared transaction is not yet replicated to the subscriber
# because two_phase is set to "false".
my $result = $node_subscriber->safe_psql('postgres',
    "SELECT count(*) FROM pg_prepared_xacts;");
is($result, q(0), "transaction is not prepared on subscriber");

$log_offset = -s $node_subscriber->logfile;

# Toggle the two_phase to "true" *before* the COMMIT PREPARED. Since we are the
# special path for the case where both two_phase and failover are altered, it
# is also set to "true".
$node_subscriber->safe_psql(
    'postgres', "
    ALTER SUBSCRIPTION regress_sub DISABLE;
    ALTER SUBSCRIPTION regress_sub SET (two_phase = true, failover = true);
    ALTER SUBSCRIPTION regress_sub ENABLE;");

# Verify the started worker recognized two_phase was enabled
$node_subscriber->wait_for_log(
	'logical replication apply worker for subscription "regress_sub" two_phase is ENABLED', $log_offset);

# And do COMMIT PREPARED the prepared transaction
$node_publisher->safe_psql('postgres',
    "COMMIT PREPARED 'test_prepared_tab_full';");
$node_publisher->wait_for_catchup('regress_sub');

# Verify inserted tuples are replicated
$result = $node_subscriber->safe_psql('postgres',
    "SELECT count(*) FROM tab_full;");
is($result, q(5),
   "prepared transactions done before altering can be replicated");

#####################
# Check the case that prepared transactions exist on the subscriber node
#
# If the two_phase is altering from "true" to "false" and there are prepared
# transactions on the subscriber, they must be aborted. This test checks it.

# Prepare a transaction to insert some tuples into the table
$node_publisher->safe_psql(
	'postgres', "
	BEGIN;
	INSERT INTO tab_full VALUES (generate_series(6, 10));
	PREPARE TRANSACTION 'test_prepared_tab_full';");

$node_publisher->wait_for_catchup('regress_sub');

# Verify the prepared transaction has been replicated to the subscriber because
# two_phase is set to "true".
$result = $node_subscriber->safe_psql('postgres',
    "SELECT count(*) FROM pg_prepared_xacts;");
is($result, q(1), "transaction has been prepared on subscriber");

# Disable the subscription to alter the two_phase option
$node_subscriber->safe_psql('postgres', "ALTER SUBSCRIPTION regress_sub DISABLE;");

# Try altering the two_phase option to "false". The command will fail since
# there is a prepared transaction and the 'force_alter' option is not specified
# as true.
my $stdout;
my $stderr;

($result, $stdout, $stderr) = $node_subscriber->psql(
	'postgres', "ALTER SUBSCRIPTION regress_sub SET (two_phase = false);");
ok($stderr =~ /cannot alter two_phase = false when there are prepared transactions/,
	'ALTER SUBSCRIPTION failed');

# Verify the prepared transaction still exists
$result = $node_subscriber->safe_psql('postgres',
    "SELECT count(*) FROM pg_prepared_xacts;");
is($result, q(1), "prepared transaction still exists");

$log_offset = -s $node_subscriber->logfile;

# Alter the two_phase true to false with the force_alter option enabled. This
# command will succeed after aborting the prepared transaction.
$node_subscriber->safe_psql('postgres',
    "ALTER SUBSCRIPTION regress_sub SET (two_phase = false, force_alter = true);");
$node_subscriber->safe_psql('postgres', "ALTER SUBSCRIPTION regress_sub ENABLE;");

# Verify the started worker recognized two_phase was disabled
$node_subscriber->wait_for_log(
	'logical replication apply worker for subscription "regress_sub" two_phase is DISABLED', $log_offset);

# # Verify the prepared transaction was aborted
$result = $node_subscriber->safe_psql('postgres',
    "SELECT count(*) FROM pg_prepared_xacts;");
is($result, q(0), "prepared transaction done by worker is aborted");

# Do COMMIT PREPARED the prepared transaction
$node_publisher->safe_psql( 'postgres',
    "COMMIT PREPARED 'test_prepared_tab_full';");
$node_publisher->wait_for_catchup('regress_sub');

# Verify inserted tuples are replicated
$result = $node_subscriber->safe_psql('postgres',
    "SELECT count(10) FROM tab_full;");
is($result, q(10),
   "prepared transactions on publisher can be replicated");

done_testing();
