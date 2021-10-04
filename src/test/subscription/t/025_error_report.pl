
# Copyright (c) 2021, PostgreSQL Global Development Group

# Tests for subscription error reporting and skipping logical
# replication transactions.

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 14;

# Test if the error reported on pg_subscription_errors view is expected.
sub test_subscription_error
{
    my ($node, $relname, $xid, $expected_error, $msg) = @_;

    my $check_sql = qq[
SELECT count(1) > 0 FROM pg_stat_subscription_errors
WHERE relid = '$relname'::regclass];
    $check_sql .= " AND xid = '$xid'::xid;" if $xid ne '';

    # Wait for the error statistics to be updated.
    $node->poll_query_until(
	'postgres', $check_sql,
) or die "Timed out while waiting for statistics to be updated";

    my $result = $node->safe_psql(
	'postgres',
	qq[
SELECT subname, command, relid::regclass, count > 0
FROM pg_stat_subscription_errors
WHERE relid = '$relname'::regclass;
]);
    is($result, $expected_error, $msg);
}
# Check the error reported on pg_stat_subscription view and skip the failed
# transaction.
sub test_skip_subscription_error
{
    my ($node, $subname, $relname, $xid, $expected_error, $msg) = @_;

    # Check the reported error.
    test_subscription_error($node, $relname, $xid, $expected_error, $msg);

    # Get XID of the failed transaction.
    my $skipxid = $node->safe_psql(
	'postgres',
	"SELECT xid FROM pg_stat_subscription_errors WHERE relid = '$relname'::regclass");
    is($skipxid, $xid, "remote xid and skip_xid are equal");

    $node->safe_psql('postgres',
		     "ALTER SUBSCRIPTION $subname SET (skip_xid = '$skipxid')");

    # Restart the subscriber node to restart logical replication with no interval.
    $node->restart;

    # Wait for the failed transaction to be skipped.
    $node->poll_query_until(
	'postgres',
	qq[
SELECT subskipxid IS NULL FROM pg_subscription
WHERE subname = '$subname'
]) or die "Timed out while waiting for the transaction to be skipped";
}

# Create publisher node.
my $node_publisher = PostgresNode->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf('postgresql.conf',
			     qq[
max_prepared_transactions = 10
logical_decoding_work_mem = 64kB
]);
$node_publisher->start;

# Create subscriber node.
my $node_subscriber = PostgresNode->new('subscriber');
$node_subscriber->init(allows_streaming => 'logical');

# The subscriber will enter an infinite error loop, so we don't want
# to overflow the server log with error messages.
$node_subscriber->append_conf('postgresql.conf',
			      qq[
max_prepared_transactions = 10
wal_retrieve_retry_interval = 5s
]);
$node_subscriber->start;

# Initial table setup on both publisher and subscriber. On subscriber we create
# the same tables but with primary keys. Also, insert some data that will conflict
# with the data replicated from publisher later.
$node_publisher->safe_psql(
    'postgres',
    q[
BEGIN;
CREATE TABLE test_tab1 (a int);
CREATE TABLE test_tab2 (a int);
CREATE TABLE test_tab_streaming (a int, b text);
INSERT INTO test_tab1 VALUES (1);
INSERT INTO test_tab2 VALUES (1);
COMMIT;
]);
$node_subscriber->safe_psql(
    'postgres',
    q[
BEGIN;
CREATE TABLE test_tab1 (a int primary key);
CREATE TABLE test_tab2 (a int primary key);
CREATE TABLE test_tab_streaming (a int primary key, b text);
INSERT INTO test_tab2 VALUES (1);
INSERT INTO test_tab_streaming SELECT 10000, md5(10000::text);
COMMIT;
]);

# Setup publications.
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
			   q[
CREATE PUBLICATION tap_pub FOR TABLE test_tab1, test_tab2;
CREATE PUBLICATION tap_pub_streaming FOR TABLE test_tab_streaming;
]);

# Check if there is no subscription errors before starting logical replication.
my $result =
    $node_subscriber->safe_psql('postgres',
				"SELECT count(1) FROM pg_stat_subscription_errors");
is($result, qq(0), 'check no subscription error');

# Create subscriptions. The table sync for test_tab2 on tap_sub will enter to
# infinite error due to violating the unique constraint.
my $appname = 'tap_sub';
$node_subscriber->safe_psql(
    'postgres',
    "CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr application_name=$appname' PUBLICATION tap_pub WITH (streaming = off, two_phase = on);");
my $appname_streaming = 'tap_sub_streaming';
$node_subscriber->safe_psql(
    'postgres',
    "CREATE SUBSCRIPTION tap_sub_streaming CONNECTION '$publisher_connstr application_name=$appname_streaming' PUBLICATION tap_pub_streaming WITH (streaming = on, two_phase = on);");

$node_publisher->wait_for_catchup($appname);
$node_publisher->wait_for_catchup($appname_streaming);

# Wait for initial table sync for test_tab1 and test_tab_streaming to finish.
$node_subscriber->poll_query_until('postgres',
				   q[
SELECT count(1) = 2 FROM pg_subscription_rel
WHERE srrelid in ('test_tab1'::regclass, 'test_tab_streaming'::regclass) AND srsubstate in ('r', 's')
]) or die "Timed out while waiting for subscriber to synchronize data";

# Check the initial data.
$result = $node_subscriber->safe_psql('postgres',
				      "SELECT count(a) FROM test_tab1");
is($result, q(1), 'check initial data are copied to subscriber');

# Insert more data to test_tab1, raising an error on the subscriber due to violation
# of the unique constraint on test_tab1.  Then skip the transaction in question.
my $xid = $node_publisher->safe_psql(
    'postgres',
    qq[
BEGIN;
INSERT INTO test_tab1 VALUES (1);
SELECT pg_current_xact_id()::xid;
COMMIT;
]);
test_skip_subscription_error($node_subscriber, 'tap_sub', 'test_tab1',
			     $xid, qq(tap_sub|INSERT|test_tab1|t),
			     'check the error reported by the apply worker');

# Check the table sync worker's error in the view.
test_subscription_error($node_subscriber, 'test_tab2', '',
			qq(tap_sub||test_tab2|t),
			'check the error reported by the table sync worker');

# Insert enough rows to test_tab_streaming to exceed the 64kB limit, also raising an
# error on the subscriber during applying spooled changes for the same reason. Then
# skip the transaction in question.
$xid = $node_publisher->safe_psql(
    'postgres',
    qq[
BEGIN;
INSERT INTO test_tab_streaming SELECT i, md5(i::text) FROM generate_series(1, 10000) s(i);
SELECT pg_current_xact_id()::xid;
COMMIT;
]);
test_skip_subscription_error($node_subscriber, 'tap_sub_streaming', 'test_tab_streaming',
			     $xid, qq(tap_sub_streaming|INSERT|test_tab_streaming|t),
			     'skip the error reported by the table sync worker during applying streaming changes');

# Insert data to test_tab1 and test_tab_streaming that don't conflict.
$node_publisher->safe_psql(
    'postgres',
    "INSERT INTO test_tab1 VALUES (2)");
$node_publisher->safe_psql(
    'postgres',
    "INSERT INTO test_tab_streaming VALUES (10001, md5(10001::text))");

$node_publisher->wait_for_catchup($appname);
$node_publisher->wait_for_catchup($appname_streaming);

# Check the data is successfully replicated after skipping the transactions.
$result = $node_subscriber->safe_psql('postgres',
				      "SELECT * FROM test_tab1");
is($result, q(1
2), "subscription gets changes after skipped transaction");
$result = $node_subscriber->safe_psql('postgres',
				      "SELECT count(1) FROM test_tab_streaming");
is($result, q(2), "subscription gets changes after skipped streamed transaction");

# Tests for skipping the transactions that are prepared and stream-prepared. We insert
# the same data as the previous tests but prepare the transactions.  Those insertions
# raise an error on the subscriptions.  Then we skip the transactions in question.
$xid = $node_publisher->safe_psql(
    'postgres',
    qq[
BEGIN;
INSERT INTO test_tab1 VALUES (1);
SELECT pg_current_xact_id()::xid;
PREPARE TRANSACTION 'skip_sub1';
COMMIT PREPARED 'skip_sub1';
]);
test_skip_subscription_error($node_subscriber, 'tap_sub', 'test_tab1',
			     $xid, qq(tap_sub|INSERT|test_tab1|t),
			     'skip the error on changes of the prepared transaction');

$xid = $node_publisher->safe_psql(
    'postgres',
    qq[
BEGIN;
INSERT INTO test_tab_streaming SELECT i, md5(i::text) FROM generate_series(1, 10000) s(i);
SELECT pg_current_xact_id()::xid;
PREPARE TRANSACTION 'skip_sub2';
COMMIT PREPARED 'skip_sub2';
]);
test_skip_subscription_error($node_subscriber, 'tap_sub_streaming', 'test_tab_streaming',
			     $xid, qq(tap_sub_streaming|INSERT|test_tab_streaming|t),
			     'skip the error on changes of the prepared-streamed transaction');

# Check if the view doesn't show any entries after dropping the subscriptions.
$node_subscriber->safe_psql(
    'postgres',
    q[
DROP SUBSCRIPTION tap_sub;
DROP SUBSCRIPTION tap_sub_streaming;
]);
$result = $node_subscriber->safe_psql('postgres',
				      "SELECT count(1) FROM pg_stat_subscription_errors");
is($result, q(0), 'no error after dropping subscription');

