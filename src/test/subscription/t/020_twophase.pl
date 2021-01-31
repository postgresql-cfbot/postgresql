# logical replication of 2PC test
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 20;

###############################
# Setup
###############################

# Initialize publisher node
my $node_publisher = get_new_node('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf('postgresql.conf',
	qq(max_prepared_transactions = 10));
$node_publisher->start;

# Create subscriber node
my $node_subscriber = get_new_node('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->append_conf('postgresql.conf',
	qq(max_prepared_transactions = 10));
$node_subscriber->start;

# Create some pre-existing content on publisher
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab_full (a int PRIMARY KEY)");
$node_publisher->safe_psql('postgres', "
	BEGIN;
	INSERT INTO tab_full SELECT generate_series(1,10);
	PREPARE TRANSACTION 'some_initial_data';
	COMMIT PREPARED 'some_initial_data';");

# Setup structure on subscriber
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab_full (a int PRIMARY KEY)");

# Setup logical replication
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub");
$node_publisher->safe_psql('postgres',
	"ALTER PUBLICATION tap_pub ADD TABLE tab_full");

my $appname = 'tap_sub';
$node_subscriber->safe_psql('postgres',	"
	CREATE SUBSCRIPTION tap_sub
	CONNECTION '$publisher_connstr application_name=$appname'
	PUBLICATION tap_pub
	WITH (two_phase = on)");

# Wait for subscriber to finish initialization
my $caughtup_query =
	"SELECT pg_current_wal_lsn() <= replay_lsn FROM pg_stat_replication WHERE application_name = '$appname';";
$node_publisher->poll_query_until('postgres', $caughtup_query)
	or die "Timed out while waiting for subscriber to catch up";

# Also wait for initial table sync to finish
my $synced_query =
	"SELECT count(1) = 0 FROM pg_subscription_rel WHERE srsubstate NOT IN ('r', 's');";
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

###############################
# check that 2PC gets replicated to subscriber
# then COMMIT PREPARED
###############################

$node_publisher->safe_psql('postgres', "
	BEGIN;
	INSERT INTO tab_full VALUES (11);
	PREPARE TRANSACTION 'test_prepared_tab_full';");

$node_publisher->poll_query_until('postgres', $caughtup_query)
	or die "Timed out while waiting for subscriber to catch up";

# check that transaction is in prepared state on subscriber
my $result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM pg_prepared_xacts where gid = 'test_prepared_tab_full';");
is($result, qq(1), 'transaction is prepared on subscriber');

# check that 2PC gets committed on subscriber
$node_publisher->safe_psql('postgres',
	"COMMIT PREPARED 'test_prepared_tab_full';");

$node_publisher->poll_query_until('postgres', $caughtup_query)
	or die "Timed out while waiting for subscriber to catch up";

# check that transaction is committed on subscriber
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM tab_full where a = 11;");
is($result, qq(1), 'Row inserted via 2PC has committed on subscriber');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM pg_prepared_xacts where gid = 'test_prepared_tab_full';");
is($result, qq(0), 'transaction is committed on subscriber');

###############################
# check that 2PC gets replicated to subscriber
# then ROLLBACK PREPARED
###############################

$node_publisher->safe_psql('postgres',"
	BEGIN;
	INSERT INTO tab_full VALUES (12);
	PREPARE TRANSACTION 'test_prepared_tab_full';");

$node_publisher->poll_query_until('postgres', $caughtup_query)
	or die "Timed out while waiting for subscriber to catch up";

# check that transaction is in prepared state on subscriber
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM pg_prepared_xacts where gid = 'test_prepared_tab_full';");
is($result, qq(1), 'transaction is prepared on subscriber');

# check that 2PC gets aborted on subscriber
$node_publisher->safe_psql('postgres',
	"ROLLBACK PREPARED 'test_prepared_tab_full';");

$node_publisher->poll_query_until('postgres', $caughtup_query)
	or die "Timed out while waiting for subscriber to catch up";

# check that transaction is aborted on subscriber
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM tab_full where a = 12;");
is($result, qq(0), 'Row inserted via 2PC is not present on subscriber');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM pg_prepared_xacts where gid = 'test_prepared_tab_full';");
is($result, qq(0), 'transaction is aborted on subscriber');

###############################
# Check that ROLLBACK PREPARED is decoded properly on crash restart
# (publisher and subscriber crash)
###############################

$node_publisher->safe_psql('postgres', "
    BEGIN;
    INSERT INTO tab_full VALUES (12);
    INSERT INTO tab_full VALUES (13);
    PREPARE TRANSACTION 'test_prepared_tab';");

$node_subscriber->stop('immediate');
$node_publisher->stop('immediate');

$node_publisher->start;
$node_subscriber->start;

# rollback post the restart
$node_publisher->safe_psql('postgres',
	"ROLLBACK PREPARED 'test_prepared_tab';");
$node_publisher->poll_query_until('postgres', $caughtup_query)
	or die "Timed out while waiting for subscriber to catch up";

# check inserts are rolled back
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM tab_full where a IN (12,13);");
is($result, qq(0), 'Rows inserted via 2PC are visible on the subscriber');

###############################
# Check that COMMIT PREPARED is decoded properly on crash restart
# (publisher and subscriber crash)
###############################

$node_publisher->safe_psql('postgres', "
    BEGIN;
    INSERT INTO tab_full VALUES (12);
    INSERT INTO tab_full VALUES (13);
    PREPARE TRANSACTION 'test_prepared_tab';");

$node_subscriber->stop('immediate');
$node_publisher->stop('immediate');

$node_publisher->start;
$node_subscriber->start;

# commit post the restart
$node_publisher->safe_psql('postgres',
	"COMMIT PREPARED 'test_prepared_tab';");
$node_publisher->poll_query_until('postgres', $caughtup_query)
	or die "Timed out while waiting for subscriber to catch up";

# check inserts are visible
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM tab_full where a IN (12,13);");
is($result, qq(2), 'Rows inserted via 2PC are visible on the subscriber');

###############################
# Check that COMMIT PREPARED is decoded properly on crash restart
# (subscriber only crash)
###############################

$node_publisher->safe_psql('postgres', "
    BEGIN;
    INSERT INTO tab_full VALUES (14);
    INSERT INTO tab_full VALUES (15);
    PREPARE TRANSACTION 'test_prepared_tab';");

$node_subscriber->stop('immediate');
$node_subscriber->start;

# commit post the restart
$node_publisher->safe_psql('postgres',
	"COMMIT PREPARED 'test_prepared_tab';");
$node_publisher->poll_query_until('postgres', $caughtup_query)
	or die "Timed out while waiting for subscriber to catch up";

# check inserts are visible
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM tab_full where a IN (14,15);");
is($result, qq(2), 'Rows inserted via 2PC are visible on the subscriber');

###############################
# Check that COMMIT PREPARED is decoded properly on crash restart
# (publisher only crash)
###############################

$node_publisher->safe_psql('postgres', "
    BEGIN;
    INSERT INTO tab_full VALUES (16);
    INSERT INTO tab_full VALUES (17);
    PREPARE TRANSACTION 'test_prepared_tab';");

$node_publisher->stop('immediate');
$node_publisher->start;

# commit post the restart
$node_publisher->safe_psql('postgres',
	"COMMIT PREPARED 'test_prepared_tab';");
$node_publisher->poll_query_until('postgres', $caughtup_query)
	or die "Timed out while waiting for subscriber to catch up";

# check inserts are visible
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM tab_full where a IN (16,17);");
is($result, qq(2), 'Rows inserted via 2PC are visible on the subscriber');

###############################
# Test nested transaction with 2PC
###############################

# check that 2PC gets replicated to subscriber
$node_publisher->safe_psql('postgres', "
	BEGIN;
	INSERT INTO tab_full VALUES (21);
	SAVEPOINT sp_inner;
	INSERT INTO tab_full VALUES (22);
	ROLLBACK TO SAVEPOINT sp_inner;
	PREPARE TRANSACTION 'outer';
	");
$node_publisher->poll_query_until('postgres', $caughtup_query)
	or die "Timed out while waiting for subscriber to catch up";

# check that transaction is in prepared state on subscriber
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM pg_prepared_xacts where gid = 'outer';");
is($result, qq(1), 'transaction is prepared on subscriber');

# COMMIT
$node_publisher->safe_psql('postgres', "
	COMMIT PREPARED 'outer';");

$node_publisher->poll_query_until('postgres', $caughtup_query)
	or die "Timed out while waiting for subscriber to catch up";

# check the tx state on subscriber
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM pg_prepared_xacts where gid = 'outer';");
is($result, qq(0), 'transaction is ended on subscriber');

# check inserts are visible. 22 should be rolled back. 21 should be committed.
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM tab_full where a IN (21);");
is($result, qq(1), 'Rows committed are on the subscriber');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM tab_full where a IN (22);");
is($result, qq(0), 'Rows rolled back are not on the subscriber');

###############################
# Test using empty GID
###############################

# check that 2PC gets replicated to subscriber
$node_publisher->safe_psql('postgres', "
	BEGIN;
	INSERT INTO tab_full VALUES (51);
	PREPARE TRANSACTION '';");
$node_publisher->poll_query_until('postgres', $caughtup_query)
	or die "Timed out while waiting for subscriber to catch up";

# check that transaction is in prepared state on subscriber
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM pg_prepared_xacts where gid = '';");
is($result, qq(1), 'transaction is prepared on subscriber');

# ROLLBACK
$node_publisher->safe_psql('postgres',
	"ROLLBACK PREPARED '';");

# check that 2PC gets aborted on subscriber
$node_publisher->poll_query_until('postgres', $caughtup_query)
	or die "Timed out while waiting for subscriber to catch up";

$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM pg_prepared_xacts where gid = '';");
is($result, qq(0), 'transaction is aborted on subscriber');

###############################
# Test cases involving DDL.
###############################

# TODO This can be added after we add functionality to replicate DDL changes to subscriber.

###############################
# check all the cleanup
###############################

$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub");

$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM pg_subscription");
is($result, qq(0), 'check subscription was dropped on subscriber');

$result = $node_publisher->safe_psql('postgres',
	"SELECT count(*) FROM pg_replication_slots");
is($result, qq(0), 'check replication slot was dropped on publisher');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM pg_subscription_rel");
is($result, qq(0),
	'check subscription relation status was dropped on subscriber');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM pg_replication_origin");
is($result, qq(0), 'check replication origin was dropped on subscriber');

$node_subscriber->stop('fast');
$node_publisher->stop('fast');
