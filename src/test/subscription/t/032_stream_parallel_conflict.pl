# Copyright (c) 2022, PostgreSQL Global Development Group

# Test for deadlock in streaming mode "parallel" in logical replication.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $offset = 0;

# Check the log that the streamed transaction was completed successfully
# reported by parallel apply worker.
sub check_parallel_log
{
	my ($node_subscriber, $offset, $is_parallel, $type) = @_;

	if ($is_parallel)
	{
		$node_subscriber->wait_for_log(
			qr/DEBUG: ( [A-Z0-9]+:)? finished processing the STREAM $type command/,
			$offset);
	}
}

# Create publisher node
my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf(
	'postgresql.conf', qq(
	logical_decoding_work_mem = 64kB
	max_prepared_transactions = 10
));
$node_publisher->start;

# Create subscriber node
my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init;

# Check if any streaming chunks are applied using the parallel apply worker.
# And check if the parallel apply worker failed to start. We have to look for
# the DEBUG1 log messages about that, so bump up the log verbosity.
$node_subscriber->append_conf(
	'postgresql.conf', qq(
	log_min_messages = debug1
	max_prepared_transactions = 10
));

$node_subscriber->start;

# Setup structure on publisher
$node_publisher->safe_psql('postgres', "CREATE TABLE test_tab (a int)");

# Setup structure on subscriber
$node_subscriber->safe_psql('postgres', "CREATE TABLE test_tab (a int)");
$node_subscriber->safe_psql('postgres',
	"CREATE UNIQUE INDEX idx_tab on test_tab(a)");

# Setup logical replication
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub FOR TABLE test_tab");

my $appname = 'tap_sub';
$node_subscriber->safe_psql(
	'postgres', "
	CREATE SUBSCRIPTION tap_sub
	CONNECTION '$publisher_connstr application_name=$appname'
	PUBLICATION tap_pub
	WITH (streaming = parallel, two_phase = on, copy_data = false)");

$node_publisher->wait_for_catchup($appname);

# Interleave a pair of transactions, each exceeding the 64kB limit.
my $in  = '';
my $out = '';

my $timer = IPC::Run::timeout($PostgreSQL::Test::Utils::timeout_default);

my $h = $node_publisher->background_psql('postgres', \$in, \$out, $timer,
	on_error_stop => 0);

# ============================================================================
# Confirm if a deadlock between the leader apply worker and the parallel apply
# worker can be detected.
# ============================================================================

$in .= q{
BEGIN;
INSERT INTO test_tab SELECT i FROM generate_series(1, 5000) s(i);
};
$h->pump_nb;

# Ensure that the parallel apply worker executes the insert command before the
# leader worker.
$node_subscriber->wait_for_log(
	qr/DEBUG: ( [A-Z0-9]+:)? applied [0-9]+ changes in the streaming chunk/,
	$offset);

$node_publisher->safe_psql('postgres', "INSERT INTO test_tab values(1)");

$in .= q{
COMMIT;
\q
};
$h->finish;

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? deadlock detected/,
	$offset);

# Drop the unique index on the subscriber, now it works.
$node_subscriber->safe_psql('postgres', "DROP INDEX idx_tab");

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

my $result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab");
is($result, qq(5001), 'data replicated to subscriber after dropping index');

# Clean up test data from the environment.
$node_publisher->safe_psql('postgres', "TRUNCATE TABLE test_tab");
$node_publisher->wait_for_catchup($appname);
$node_subscriber->safe_psql('postgres',
	"CREATE UNIQUE INDEX idx_tab on test_tab(a)");

# ============================================================================
# Confirm if a deadlock between two parallel apply workers can be detected.
# ============================================================================

# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$in .= q{
BEGIN;
INSERT INTO test_tab SELECT i FROM generate_series(1, 5000) s(i);
};
$h->pump_nb;

# Ensure that the first parallel apply worker executes the insert command
# before the second one.
$node_subscriber->wait_for_log(
	qr/DEBUG: ( [A-Z0-9]+:)? applied [0-9]+ changes in the streaming chunk/,
	$offset);

$node_publisher->safe_psql('postgres', "INSERT INTO test_tab SELECT i FROM generate_series(1, 5000) s(i)");

$in .= q{
COMMIT;
\q
};
$h->finish;

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? deadlock detected/,
	$offset);

# Drop the unique index on the subscriber, now it works.
$node_subscriber->safe_psql('postgres', "DROP INDEX idx_tab");

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab");
is($result, qq(10000), 'data replicated to subscriber after dropping index');

# Clean up test data from the environment.
$node_publisher->safe_psql('postgres', "TRUNCATE TABLE test_tab");
$node_publisher->wait_for_catchup($appname);

# ============================================================================
# Test serializing messages to disk
# ============================================================================

# Set stream_serialize_threshold to zero, so the messages will be serialized to disk.
$node_subscriber->safe_psql('postgres',
	'ALTER SYSTEM SET stream_serialize_threshold = 0;');
$node_subscriber->reload;

# Serialize the COMMIT transaction.
# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$node_publisher->safe_psql('postgres', "INSERT INTO test_tab SELECT i FROM generate_series(1, 5000) s(i)");

# Ensure that the messages are serialized.
$node_subscriber->wait_for_log(
	qr/DEBUG: ( [A-Z0-9]+:)? opening file ".*\.changes" for streamed changes/,
	$offset);

$node_publisher->wait_for_catchup($appname);

# Check that transaction is committed on subscriber
$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab");
is($result, qq(5000), 'data replicated to subscriber by serializing messages to disk');

# Clean up test data from the environment.
$node_publisher->safe_psql('postgres', "TRUNCATE TABLE test_tab");
$node_publisher->wait_for_catchup($appname);

# Serialize the PREPARE transaction.
# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$node_publisher->safe_psql(
	'postgres', q{
	BEGIN;
	INSERT INTO test_tab SELECT i FROM generate_series(1, 5000) s(i);
	PREPARE TRANSACTION 'xact';
	});

# Ensure that the messages are serialized.
$node_subscriber->wait_for_log(
	qr/DEBUG: ( [A-Z0-9]+:)? opening file ".*\.changes" for streamed changes/,
	$offset);

$node_publisher->wait_for_catchup($appname);

# Check that transaction is in prepared state on subscriber
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM pg_prepared_xacts;");
is($result, qq(1), 'transaction is prepared on subscriber');

# Check that 2PC gets committed on subscriber
$node_publisher->safe_psql('postgres',
	"COMMIT PREPARED 'xact';");

$node_publisher->wait_for_catchup($appname);

# Check that transaction is committed on subscriber
$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab");
is($result, qq(5000), 'data replicated to subscriber by serializing messages to disk');

# Clean up test data from the environment.
$node_publisher->safe_psql('postgres', "TRUNCATE TABLE test_tab");
$node_publisher->wait_for_catchup($appname);

# Serialize the ABORT top-transaction.
# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$node_publisher->safe_psql(
	'postgres', q{
	BEGIN;
	INSERT INTO test_tab SELECT i FROM generate_series(1, 5000) s(i);
	ROLLBACK;
	});

# Ensure that the messages are serialized.
$node_subscriber->wait_for_log(
	qr/DEBUG: ( [A-Z0-9]+:)? opening file ".*\.changes" for streamed changes/,
	$offset);

$node_publisher->wait_for_catchup($appname);

# Check that transaction is aborted on subscriber
$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab");
is($result, qq(0), 'data replicated to subscriber by serializing messages to disk');

# Clean up test data from the environment.
$node_publisher->safe_psql('postgres', "TRUNCATE TABLE test_tab");
$node_publisher->wait_for_catchup($appname);

# Serialize the ABORT sub-transaction.
# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$node_publisher->safe_psql(
	'postgres', q{
	BEGIN;
	INSERT INTO test_tab SELECT i FROM generate_series(1, 5000) s(i);
	SAVEPOINT sp;
	INSERT INTO test_tab SELECT i FROM generate_series(5001, 10000) s(i);
	ROLLBACK TO sp;
	COMMIT;
	});

# Ensure that the messages are serialized.
$node_subscriber->wait_for_log(
	qr/DEBUG: ( [A-Z0-9]+:)? opening file ".*\.changes" for streamed changes/,
	$offset);

$node_publisher->wait_for_catchup($appname);

# Check that only sub-transaction is aborted on subscriber.
$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab");
is($result, qq(5000), 'data replicated to subscriber by serializing messages to disk');

# Clean up test data from the environment.
$node_publisher->safe_psql('postgres', "TRUNCATE TABLE test_tab");
$node_publisher->wait_for_catchup($appname);

# ============================================================================
# Test re-apply a failed streaming transaction using the leader apply
# worker and apply subsequent streaming transaction using the parallel apply
# worker after this retry succeeds.
# ============================================================================

$node_subscriber->safe_psql('postgres',
	"CREATE UNIQUE INDEX idx_tab on test_tab(a)");

# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$node_publisher->safe_psql(
	'postgres', qq{
BEGIN;
INSERT INTO test_tab SELECT i FROM generate_series(1, 5000) s(i);
INSERT INTO test_tab values(1);
COMMIT;});

# Check if the parallel apply worker is not started because the above
# transaction failed to be applied.
$node_subscriber->wait_for_log(
	qr/DEBUG: ( [A-Z0-9]+:)? parallel apply workers are not used for retries/,
	$offset);

# Drop the unique index on the subscriber, now it works.
$node_subscriber->safe_psql('postgres', "DROP INDEX idx_tab");

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab");
is($result, qq(5001), 'data replicated to subscriber after dropping index');

# After successfully retrying to apply a failed streaming transaction, apply
# the following streaming transaction using the parallel apply worker.
$node_publisher->safe_psql('postgres', "INSERT INTO test_tab SELECT i FROM generate_series(5001, 10000) s(i)");

check_parallel_log($node_subscriber, $offset, 1, 'COMMIT');

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab");
is($result, qq(10001), 'data replicated to subscriber using the parallel apply worker');

$node_subscriber->stop;
$node_publisher->stop;

done_testing();
