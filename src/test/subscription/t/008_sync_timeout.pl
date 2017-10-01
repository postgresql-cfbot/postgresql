# Tests for logical replication table syncing
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 3;

# Initialize publisher node
my $node_publisher = get_new_node('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf('postgresql.conf',
	"wal_sender_timeout = 10ms");
$node_publisher->start;

# Create subscriber node
my $node_subscriber = get_new_node('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->append_conf('postgresql.conf',
	"wal_retrieve_retry_interval = 1ms");
$node_subscriber->start;

# Create some preexisting content on publisher
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab_rep (a int primary key)");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rep SELECT generate_series(1,10)");

# Setup structure on subscriber
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab_rep (a int primary key)");

# Setup logical replication
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub FOR ALL TABLES");

my $appname = 'tap_sub';
$node_subscriber->safe_psql('postgres',
"CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr application_name=$appname' PUBLICATION tap_pub"
);

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

my $result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM tab_rep");
is($result, qq(10), 'initial data synced for first sub');

$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rep SELECT generate_series(11,20000)");

# wait for sync to finish this time
$node_publisher->poll_query_until('postgres', $caughtup_query)
  or die "Timed out while waiting for subscriber to catch up";
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

# check that all data is synced
$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM tab_rep");
is($result, qq(20000), 'initial data synced for second sub');

# stricter timeout
$node_publisher->append_conf('postgresql.conf',
	"wal_sender_timeout = 1ms");
$node_publisher->reload;

$node_publisher->safe_psql('postgres', "DELETE FROM tab_rep");

# wait for sync to finish this time
$node_publisher->poll_query_until('postgres', $caughtup_query)
  or die "Timed out while waiting for subscriber to catch up";
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

# check that all data is synced
$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM tab_rep");
is($result, qq(0), 'initial data synced for second sub');

$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub");

$node_subscriber->stop('fast');
$node_publisher->stop('fast');
