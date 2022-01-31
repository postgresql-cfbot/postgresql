
# Copyright (c) 2022, PostgreSQL Global Development Group

# Tests for skipping logical replication transactions
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use Test::More tests => 7;

# Test skipping the transaction. This function must be called after the caller
# has inserted data that conflicts with the subscriber.  After waiting for the
# subscription worker stats to be updated, we skip the transaction in question
# by ALTER SUBSCRIPTION ... SKIP. Then, check if logical replication can continue
# working by inserting $nonconflict_data on the publisher.
sub test_skip_xact
{
	my ($node_publisher, $node_subscriber, $subname, $relname,
		$nonconflict_data, $expected, $xid, $msg)
	  = @_;

	local $Test::Builder::Level = $Test::Builder::Level + 1;

	# Wait for worker error
	$node_subscriber->poll_query_until(
		'postgres',
		qq[
SELECT count(1) > 0
FROM pg_stat_subscription_workers
WHERE last_error_relid = '$relname'::regclass
    AND subrelid IS NULL
    AND last_error_command = 'INSERT'
    AND last_error_xid = '$xid'
    AND starts_with(last_error_message, 'duplicate key value violates unique constraint');
]) or die "Timed out while waiting for worker error";

	# Set skip xid
	$node_subscriber->safe_psql('postgres',
		"ALTER SUBSCRIPTION $subname SKIP (xid = '$xid')");

	# Restart the subscriber node to restart logical replication with no interval
	$node_subscriber->restart;

	# Wait for the failed transaction to be skipped
	$node_subscriber->poll_query_until('postgres',
		"SELECT subskipxid = 0 FROM pg_subscription WHERE subname = '$subname'"
	);

	# Insert non-conflict data
	$node_publisher->safe_psql('postgres',
		"INSERT INTO $relname VALUES $nonconflict_data");

	$node_publisher->wait_for_catchup($subname);

	# Check replicated data
	my $res = $node_subscriber->safe_psql('postgres',
		"SELECT count(*) FROM $relname");
	is($res, $expected, $msg);
}

# Create publisher node. Set a low value to logical_decoding_work_mem
# so we can test streaming cases easily.
my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf(
	'postgresql.conf',
	qq[
logical_decoding_work_mem = 64kB
max_prepared_transactions = 10
]);
$node_publisher->start;

# Create subscriber node
my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->append_conf(
	'postgresql.conf',
	qq[
max_prepared_transactions = 10
]);

# The subscriber will enter an infinite error loop, so we don't want
# to overflow the server log with error messages.
$node_subscriber->append_conf(
	'postgresql.conf',
	qq[
wal_retrieve_retry_interval = 2s
]);
$node_subscriber->start;

# Initial table setup on both publisher and subscriber. On the subscriber, we
# create the same tables but with primary keys. Also, insert some data that
# will conflict with the data replicated from publisher later.
$node_publisher->safe_psql(
	'postgres',
	qq[
BEGIN;
CREATE TABLE test_tab (a int);
CREATE TABLE test_tab_streaming (a int, b text);
COMMIT;
]);
$node_subscriber->safe_psql(
	'postgres',
	qq[
BEGIN;
CREATE TABLE test_tab (a int primary key);
CREATE TABLE test_tab_streaming (a int primary key, b text);
INSERT INTO test_tab VALUES (1);
INSERT INTO test_tab_streaming VALUES (1, md5(1::text));
COMMIT;
]);

# Setup publications
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql(
	'postgres',
	qq[
CREATE PUBLICATION tap_pub FOR TABLE test_tab;
CREATE PUBLICATION tap_pub_streaming FOR TABLE test_tab_streaming;
]);

# Create subscriptions
$node_subscriber->safe_psql(
	'postgres',
	qq[
CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub WITH (two_phase = on);
CREATE SUBSCRIPTION tap_sub_streaming CONNECTION '$publisher_connstr' PUBLICATION tap_pub_streaming WITH (two_phase = on, streaming = on);
]);

$node_publisher->wait_for_catchup('tap_sub');
$node_publisher->wait_for_catchup('tap_sub_streaming');

# Insert data to test_tab1, raising an error on the subscriber due to violation
# of the unique constraint on test_tab. Then skip the transaction.
my $xid = $node_publisher->safe_psql(
	'postgres',
	qq[
BEGIN;
INSERT INTO test_tab VALUES (1);
SELECT pg_current_xact_id()::xid;
COMMIT;
]);
test_skip_xact($node_publisher, $node_subscriber, "tap_sub", "test_tab",
	"(2)", "2", $xid, "test skipping transaction");

# Test for PREPARE and COMMIT PREPARED. Insert the same data to test_tab1 and
# PREPARE the transaction, raising an error. Then skip the transaction.
$xid = $node_publisher->safe_psql(
	'postgres',
	qq[
BEGIN;
INSERT INTO test_tab VALUES (1);
SELECT pg_current_xact_id()::xid;
PREPARE TRANSACTION 'gtx';
COMMIT PREPARED 'gtx';
]);
test_skip_xact($node_publisher, $node_subscriber, "tap_sub", "test_tab",
	"(3)", "3", $xid, "test skipping prepare and commit prepared ");

# Test for PREPARE and ROLLBACK PREPARED
$xid = $node_publisher->safe_psql(
	'postgres',
	qq[
BEGIN;
INSERT INTO test_tab VALUES (1);
SELECT pg_current_xact_id()::xid;
PREPARE TRANSACTION 'gtx';
ROLLBACK PREPARED 'gtx';
]);
test_skip_xact($node_publisher, $node_subscriber, "tap_sub", "test_tab",
	"(4)", "4", $xid, "test skipping prepare and rollback prepared");

# Test for STREAM COMMIT. Insert enough rows to test_tab_streaming to exceed the 64kB
# limit, also raising an error on the subscriber during applying spooled changes for the
# same reason. Then skip the transaction.
$xid = $node_publisher->safe_psql(
	'postgres',
	qq[
BEGIN;
INSERT INTO test_tab_streaming SELECT i, md5(i::text) FROM generate_series(1, 10000) s(i);
SELECT pg_current_xact_id()::xid;
COMMIT;
]);
test_skip_xact($node_publisher, $node_subscriber, "tap_sub_streaming",
	"test_tab_streaming", "(2, md5(2::text))",
	"2", $xid, "test skipping stream-commit");

# Test for STREAM PREPARE and COMMIT PREPARED
$xid = $node_publisher->safe_psql(
	'postgres',
	qq[
BEGIN;
INSERT INTO test_tab_streaming SELECT i, md5(i::text) FROM generate_series(1, 10000) s(i);
SELECT pg_current_xact_id()::xid;
PREPARE TRANSACTION 'gtx';
COMMIT PREPARED 'gtx';
]);
test_skip_xact(
	$node_publisher,     $node_subscriber,
	"tap_sub_streaming", "test_tab_streaming",
	"(3, md5(3::text))", "3",
	$xid,                "test skipping stream-prepare and commit prepared");

# Test for STREAM PREPARE and ROLLBACK PREPARED
$xid = $node_publisher->safe_psql(
	'postgres',
	qq[
BEGIN;
INSERT INTO test_tab_streaming SELECT i, md5(i::text) FROM generate_series(1, 10000) s(i);
SELECT pg_current_xact_id()::xid;
PREPARE TRANSACTION 'gtx';
ROLLBACK PREPARED 'gtx';
]);
test_skip_xact(
	$node_publisher,
	$node_subscriber,
	"tap_sub_streaming",
	"test_tab_streaming",
	"(4, md5(4::text))",
	"4",
	$xid,
	"test skipping stream-prepare and rollback prepared");

my $res = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM pg_prepared_xacts");
is($res, "0",
	"check all prepared transactions are resolved on the subscriber");
