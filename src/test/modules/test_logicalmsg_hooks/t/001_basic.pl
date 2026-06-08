# Copyright (c) 2021-2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $pub = PostgreSQL::Test::Cluster->new('publisher');
$pub->init(allows_streaming => 'logical');
$pub->start;

my $sub = PostgreSQL::Test::Cluster->new('subscriber');
$sub->init(allows_streaming => 'logical');
$sub->append_conf(
	'postgresql.conf', q{
shared_preload_libraries = 'test_logicalmsg_hooks'
});
$sub->start;

$pub->safe_psql(
	'postgres',
	qq{
CREATE TABLE test (a int);
INSERT INTO test VALUES (1);
CREATE PUBLICATION pub FOR ALL TABLES;
});

my $pub_connstr = $pub->connstr . ' dbname=postgres';

$sub->safe_psql(
	'postgres',
	qq{
CREATE TABLE test (a int primary key);
CREATE SUBSCRIPTION sub CONNECTION '$pub_connstr' PUBLICATION pub WITH (message = true, disable_on_error = true)
});

$sub->wait_for_subscription_sync($pub, 'sub');

my $log_location = -s $sub->logfile;

# Emit a logical message and verify that the subscriber processed it in the
# hook function.
$pub->safe_psql(
	'postgres',
	qq{
SELECT pg_logical_emit_message(true, 'test_prefix', 'test_mesasge');
});
$pub->wait_for_catchup('sub');
$sub->wait_for_log(
	qr/LOG[^\n]+received message: LSN [[:xdigit:]]+\/[[:xdigit:]]+, prefix: test_prefix, message: test_mesasge/,
	$log_location);
ok(1, "logical message is correctly handled on the subscriber");

$log_location = -s $sub->logfile;

# INSERT a tuple with a=1 conflicts on the subscriber. Test that ALTER SUBSCRIPTION ... SKIP
# the transacitonal message as well.
$pub->safe_psql(
	'postgres',
	q{
BEGIN;
INSERT INTO test VALUES (1);
SELECT pg_logical_emit_message(true, 'conflict', 'should be skipped');
COMMIT;
});

# Wait until a conflict occurs on the subscriber.
$sub->poll_query_until('postgres',
	q{SELECT subenabled = FALSE FROM pg_subscription WHERE subname = 'sub'});

# Get the finish LSN of the error transaction.
my $contents = slurp_file($sub->logfile, $log_location);
$contents =~
  qr/conflict detected on relation "public.test".*\n.*DETAIL:.*Could not apply remote change.*\n.*Key already exists in unique index "test_pkey", modified in transaction \d+: key .*, local row .*\n.*CONTEXT:.* for replication target relation "public.test" in transaction \d+, finished at ([[:xdigit:]]+\/[[:xdigit:]]+)/m
  or die "could not get error-LSN";
my $lsn = $1;

$log_location = -s $sub->logfile;

# Set skip LSN and re-enable the subscription.
$sub->safe_psql('postgres', qq{ALTER SUBSCRIPTION sub SKIP (lsn = '$lsn');});
$sub->safe_psql('postgres', "ALTER SUBSCRIPTION sub ENABLE");

# Wait for the failed transaction to be skipped
$sub->poll_query_until('postgres',
	q{SELECT subskiplsn = '0/0' FROM pg_subscription WHERE subname = 'sub'});

ok( !$sub->log_contains(
		qr/LOG[^\n]+received message: LSN .*, prefix: conflict, message: should be skipped/
	),
	"logical message is skipped by ALTER SUBSCRIPTION SKIP");

# Test that a non-transactional message is applied even if the transaction rolls back.
$log_location = -s $sub->logfile;
$pub->safe_psql(
	'postgres',
	q{
BEGIN;
SELECT pg_logical_emit_message(false, 'test_prefix', 'non-transactional message');
ROLLBACK;
});
$pub->wait_for_catchup('sub');
$sub->wait_for_log(
	qr/LOG[^\n]+received message: LSN [[:xdigit:]]+\/[[:xdigit:]]+, prefix: test_prefix, message: non-transactional message/,
	$log_location);
ok(1, "logical message is correctly handled on the subscriber");


$pub->stop;
$sub->stop;
done_testing();
