# Test cascading logical replication of 2PC.
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 31;

###############################
# Setup a cascade of pub/sub nodes.
# node_A -> node_B -> node_C
###############################

# Initialize nodes
# node_A
my $node_A = get_new_node('node_A');
$node_A->init(allows_streaming => 'logical');
$node_A->append_conf('postgresql.conf', qq(max_prepared_transactions = 10));
$node_A->append_conf('postgresql.conf', qq(logical_decoding_work_mem = 64kB));
$node_A->start;
# node_B
my $node_B = get_new_node('node_B');
$node_B->init(allows_streaming => 'logical');
$node_B->append_conf('postgresql.conf', qq(max_prepared_transactions = 10));
$node_B->append_conf('postgresql.conf', qq(logical_decoding_work_mem = 64kB));
$node_B->start;
# node_C
my $node_C = get_new_node('node_C');
$node_C->init(allows_streaming => 'logical');
$node_C->append_conf('postgresql.conf', qq(max_prepared_transactions = 10));
$node_C->append_conf('postgresql.conf', qq(logical_decoding_work_mem = 64kB));
$node_C->start;

# Create some pre-existing content on node_A (uses same DDL as 015_stream.pl)
$node_A->safe_psql('postgres',
	"CREATE TABLE test_tab (a int primary key, b varchar)");
$node_A->safe_psql('postgres',
	"INSERT INTO test_tab VALUES (1, 'foo'), (2, 'bar')");

# Create the same tables on node_B amd node_C
# columns a and b are compatible with same table name on node_A
$node_B->safe_psql('postgres',
	"CREATE TABLE test_tab (a int primary key, b text, c timestamptz DEFAULT now(), d bigint DEFAULT 999)");
$node_C->safe_psql('postgres',
	"CREATE TABLE test_tab (a int primary key, b text, c timestamptz DEFAULT now(), d bigint DEFAULT 999)");

# Setup logical replication (streaming = on)

# node_A (pub) -> node_B (sub)
my $node_A_connstr = $node_A->connstr . ' dbname=postgres';
$node_A->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub_A");
$node_A->safe_psql('postgres',
	"ALTER PUBLICATION tap_pub_A ADD TABLE test_tab");
my $appname_B = 'tap_sub_B';
$node_B->safe_psql('postgres',	"
	CREATE SUBSCRIPTION tap_sub_B
	CONNECTION '$node_A_connstr application_name=$appname_B'
	PUBLICATION tap_pub_A
	WITH (streaming = on, two_phase = on)");

# node_B (pub) -> node_C (sub)
my $node_B_connstr = $node_B->connstr . ' dbname=postgres';
$node_B->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub_B");
$node_B->safe_psql('postgres',
	"ALTER PUBLICATION tap_pub_B ADD TABLE test_tab");
my $appname_C = 'tap_sub_C';
$node_C->safe_psql('postgres',	"
	CREATE SUBSCRIPTION tap_sub_C
	CONNECTION '$node_B_connstr application_name=$appname_C'
	PUBLICATION tap_pub_B
	WITH (streaming = on, two_phase = on)");

# Wait for subscribers to finish initialization
my $caughtup_query_B =
	"SELECT pg_current_wal_lsn() <= replay_lsn FROM pg_stat_replication WHERE application_name = '$appname_B';";
$node_A->poll_query_until('postgres', $caughtup_query_B)
	or die "Timed out while waiting for subscriber B to catch up";
my $caughtup_query_C =
	"SELECT pg_current_wal_lsn() <= replay_lsn FROM pg_stat_replication WHERE application_name = '$appname_C';";
$node_B->poll_query_until('postgres', $caughtup_query_C)
	or die "Timed out while waiting for subscriber C to catch up";

# Wait for initial table syncs to finish
my $synced_query =
	"SELECT count(1) = 0 FROM pg_subscription_rel WHERE srsubstate NOT IN ('r', 's');";
$node_B->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber B to synchronize data";
$node_C->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber C to synchronize data";

is(1, 1, "Cascaded setup is complete");

my $result;

###############################
# Check initial data was copied to subscriber(s)
###############################
$result = $node_B->safe_psql('postgres',
	"SELECT count(*), count(c), count(d = 999) FROM test_tab");
is($result, qq(2|2|2), 'check initial data was copied to subscriber B');
$result = $node_C->safe_psql('postgres',
	"SELECT count(*), count(c), count(d = 999) FROM test_tab");
is($result, qq(2|2|2), 'check initial data was copied to subscriber B');

###############################
# Test 2PC PREPARE / COMMIT PREPARED
# 1. Data is streamed as a 2PC transaction.
# 2. Then do commit prepared.
# Expect all data is replicated on subscriber(s) after the commit.
###############################

# Insert, update and delete enough rows to exceed the 64kB limit.
# Then 2PC PREPARE
$node_A->safe_psql('postgres', q{
	BEGIN;
	INSERT INTO test_tab SELECT i, md5(i::text) FROM generate_series(3, 5000) s(i);
	UPDATE test_tab SET b = md5(b) WHERE mod(a,2) = 0;
	DELETE FROM test_tab WHERE mod(a,3) = 0;
	PREPARE TRANSACTION 'test_prepared_tab';});
$node_A->poll_query_until('postgres', $caughtup_query_B)
	or die "Timed out while waiting for subscriber B to catch up";
$node_B->poll_query_until('postgres', $caughtup_query_C)
	or die "Timed out while waiting for subscriber C to catch up";

# check the tx state is prepared on subscriber(s)
$result = $node_B->safe_psql('postgres',
	"SELECT count(*) FROM pg_prepared_xacts where gid = 'test_prepared_tab';");
is($result, qq(1), 'transaction is prepared on subscriber B');
$result = $node_C->safe_psql('postgres',
	"SELECT count(*) FROM pg_prepared_xacts where gid = 'test_prepared_tab';");
is($result, qq(1), 'transaction is prepared on subscriber C');

# 2PC COMMIT
$node_A->safe_psql('postgres',
	"COMMIT PREPARED 'test_prepared_tab';");
$node_A->poll_query_until('postgres', $caughtup_query_B)
	or die "Timed out while waiting for subscriber B to catch up";
$node_B->poll_query_until('postgres', $caughtup_query_C)
	or die "Timed out while waiting for subscriber C to catch up";

# check that transaction was committed on subscriber(s)
$result = $node_B->safe_psql('postgres',
	"SELECT count(*), count(c), count(d = 999) FROM test_tab");
is($result, qq(3334|3334|3334), 'Rows inserted by 2PC have committed on subscriber B, and extra columns have local defaults');
$result = $node_C->safe_psql('postgres',
	"SELECT count(*), count(c), count(d = 999) FROM test_tab");
is($result, qq(3334|3334|3334), 'Rows inserted by 2PC have committed on subscriber C, and extra columns have local defaults');

# check the tx state is ended on subscriber(s)
$result = $node_B->safe_psql('postgres',
	"SELECT count(*) FROM pg_prepared_xacts where gid = 'test_prepared_tab';");
is($result, qq(0), 'transaction is committed on subscriber B');
$result = $node_C->safe_psql('postgres',
	"SELECT count(*) FROM pg_prepared_xacts where gid = 'test_prepared_tab';");
is($result, qq(0), 'transaction is committed on subscriber C');

###############################
# Test 2PC PREPARE / ROLLBACK PREPARED.
# 1. Table is deleted back to 2 rows which are replicated on subscriber.
# 2. Data is streamed using 2PC
# 3. Do rollback prepared.
# Expect data rolls back leaving only the original 2 rows.
###############################

# First, delete the data except for 2 rows (delete will be replicated)
$node_A->safe_psql('postgres', q{
    DELETE FROM test_tab WHERE a > 2;});

# Insert, update and delete enough rows to exceed the 64kB limit.
# The 2PC PREPARE
$node_A->safe_psql('postgres', q{
	BEGIN;
	INSERT INTO test_tab SELECT i, md5(i::text) FROM generate_series(3, 5000) s(i);
	UPDATE test_tab SET b = md5(b) WHERE mod(a,2) = 0;
	DELETE FROM test_tab WHERE mod(a,3) = 0;
	PREPARE TRANSACTION 'test_prepared_tab';});
$node_A->poll_query_until('postgres', $caughtup_query_B)
	or die "Timed out while waiting for subscriber B to catch up";
$node_B->poll_query_until('postgres', $caughtup_query_C)
	or die "Timed out while waiting for subscriber C to catch up";

# check the tx state is prepared on subscriber(s)
$result = $node_B->safe_psql('postgres',
	"SELECT count(*) FROM pg_prepared_xacts where gid = 'test_prepared_tab';");
is($result, qq(1), 'transaction is prepared on subscriber B');
$result = $node_C->safe_psql('postgres',
	"SELECT count(*) FROM pg_prepared_xacts where gid = 'test_prepared_tab';");
is($result, qq(1), 'transaction is prepared on subscriber C');

# 2PC ROLLBACK
$node_A->safe_psql('postgres',
	"ROLLBACK PREPARED 'test_prepared_tab';");
$node_A->poll_query_until('postgres', $caughtup_query_B)
	or die "Timed out while waiting for subscriber B to catch up";
$node_B->poll_query_until('postgres', $caughtup_query_C)
	or die "Timed out while waiting for subscriber C to catch up";

# check that transaction is aborted on subscriber(s)
$result = $node_B->safe_psql('postgres',
	"SELECT count(*), count(c), count(d = 999) FROM test_tab");
is($result, qq(2|2|2), 'Row inserted by 2PC is not present. Only initial data remains on subscriber B');
$result = $node_C->safe_psql('postgres',
	"SELECT count(*), count(c), count(d = 999) FROM test_tab");
is($result, qq(2|2|2), 'Row inserted by 2PC is not present. Only initial data remains on subscriber C');

# check the tx state is ended on subscriber(s)
$result = $node_B->safe_psql('postgres',
	"SELECT count(*) FROM pg_prepared_xacts where gid = 'test_prepared_tab';");
is($result, qq(0), 'transaction is ended on subscriber B');
$result = $node_C->safe_psql('postgres',
	"SELECT count(*) FROM pg_prepared_xacts where gid = 'test_prepared_tab';");
is($result, qq(0), 'transaction is ended on subscriber C');

###############################
# Test 2PC PREPARE with a nested ROLLBACK TO SAVEPOINT.
# 0. There are 2 rows only in the table (from previous test)
# 1. Insert one more row
# 2. Record a SAVEPOINT
# 3. Data is streamed using 2PC
# 4. Do rollback to SAVEPOINT prior to the streamed inserts
# 5. Then COMMIT PRPEARED
# Expect data after the SAVEPOINT is aborted leaving only 3 rows (= 2 original + 1 from step 1)
###############################

# 2PC PREPARE with a nested ROLLBACK TO SAVEPOINT
$node_A->safe_psql('postgres', "
	BEGIN;
	INSERT INTO test_tab VALUES (9999, 'foobar');
	SAVEPOINT sp_inner;
	INSERT INTO test_tab SELECT i, md5(i::text) FROM generate_series(3, 5000) s(i);
	UPDATE test_tab SET b = md5(b) WHERE mod(a,2) = 0;
	DELETE FROM test_tab WHERE mod(a,3) = 0;
	ROLLBACK TO SAVEPOINT sp_inner;
	PREPARE TRANSACTION 'outer';
	");
$node_A->poll_query_until('postgres', $caughtup_query_B)
	or die "Timed out while waiting for subscriber B to catch up";
$node_B->poll_query_until('postgres', $caughtup_query_C)
	or die "Timed out while waiting for subscriber C to catch up";

# check the tx state prepared on subscriber(s)
$result = $node_B->safe_psql('postgres',
	"SELECT count(*) FROM pg_prepared_xacts where gid = 'outer';");
is($result, qq(1), 'transaction is prepared on subscriber B');
$result = $node_C->safe_psql('postgres',
	"SELECT count(*) FROM pg_prepared_xacts where gid = 'outer';");
is($result, qq(1), 'transaction is prepared on subscriber C');

# 2PC COMMIT
$node_A->safe_psql('postgres', "
	COMMIT PREPARED 'outer';");
$node_A->poll_query_until('postgres', $caughtup_query_B)
	or die "Timed out while waiting for subscriber B to catch up";
$node_B->poll_query_until('postgres', $caughtup_query_C)
	or die "Timed out while waiting for subscriber C to catch up";

# check the tx state is ended on subscriber
$result = $node_B->safe_psql('postgres',
	"SELECT count(*) FROM pg_prepared_xacts where gid = 'outer';");
is($result, qq(0), 'transaction is ended on subscriber B');
$result = $node_C->safe_psql('postgres',
	"SELECT count(*) FROM pg_prepared_xacts where gid = 'outer';");
is($result, qq(0), 'transaction is ended on subscriber C');

# check inserts are visible at subscriber(s).
# All the streamed data (prior to the SAVEPOINT) should be rolled back.
# (3, 'foobar') should be committed.
$result = $node_B->safe_psql('postgres',
	"SELECT count(*) FROM test_tab where b = 'foobar';");
is($result, qq(1), 'Rows committed are present on subscriber B');
$result = $node_B->safe_psql('postgres',
	"SELECT count(*) FROM test_tab;");
is($result, qq(3), 'Rows rolled back are not present on subscriber B');
$result = $node_C->safe_psql('postgres',
	"SELECT count(*) FROM test_tab where b = 'foobar';");
is($result, qq(1), 'Rows committed are present on subscriber C');
$result = $node_C->safe_psql('postgres',
	"SELECT count(*) FROM test_tab;");
is($result, qq(3), 'Rows rolled back are not present on subscriber C');

###############################
# check all the cleanup
###############################

# cleanup the node_B => node_C pub/sub
$node_C->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub_C");
$result = $node_C->safe_psql('postgres',
	"SELECT count(*) FROM pg_subscription");
is($result, qq(0), 'check subscription was dropped on subscriber node C');
$result = $node_C->safe_psql('postgres',
	"SELECT count(*) FROM pg_subscription_rel");
is($result, qq(0), 'check subscription relation status was dropped on subscriber node C');
$result = $node_C->safe_psql('postgres',
	"SELECT count(*) FROM pg_replication_origin");
is($result, qq(0), 'check replication origin was dropped on subscriber node C');
$result = $node_B->safe_psql('postgres',
	"SELECT count(*) FROM pg_replication_slots");
is($result, qq(0), 'check replication slot was dropped on publisher node B');

# cleanup the node_A => node_B pub/sub
$node_B->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub_B");
$result = $node_B->safe_psql('postgres',
	"SELECT count(*) FROM pg_subscription");
is($result, qq(0), 'check subscription was dropped on subscriber node B');
$result = $node_B->safe_psql('postgres',
	"SELECT count(*) FROM pg_subscription_rel");
is($result, qq(0), 'check subscription relation status was dropped on subscriber node B');
$result = $node_B->safe_psql('postgres',
	"SELECT count(*) FROM pg_replication_origin");
is($result, qq(0), 'check replication origin was dropped on subscriber node B');
$result = $node_A->safe_psql('postgres',
	"SELECT count(*) FROM pg_replication_slots");
is($result, qq(0), 'check replication slot was dropped on publisher node A');

# shutdown
$node_C->stop('fast');
$node_B->stop('fast');
$node_A->stop('fast');
