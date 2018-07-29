# Basic logical replication test
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 1;

# Initialize publisher node
my $node_publisher = get_new_node('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

# Create subscriber node
my $node_subscriber = get_new_node('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->start;

# Create structure on both sides
$node_publisher->safe_psql('postgres',
	'CREATE FUNCTION my_constr(int) RETURNS bool LANGUAGE sql AS $$ select count(*) > 0 from pg_authid; $$;
	 CREATE DOMAIN pos_int AS int CHECK (my_constr(VALUE));
	 CREATE TABLE tab_rep (a pos_int);
	 INSERT INTO tab_rep VALUES (10)');

$node_subscriber->safe_psql('postgres',
	'CREATE FUNCTION my_constr(int) RETURNS bool LANGUAGE sql AS $$ select count(*) > 0 from pg_authid; $$;
	 CREATE DOMAIN pos_int AS int CHECK (my_constr(VALUE));
	 CREATE TABLE tab_rep (a pos_int);');

# Setup logical replication
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres', "CREATE PUBLICATION tap_pub");
$node_publisher->safe_psql('postgres',
"ALTER PUBLICATION tap_pub ADD TABLE tab_rep"
);

my $appname = 'tap_sub';
$node_subscriber->safe_psql('postgres',
"CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr application_name=$appname' PUBLICATION tap_pub"
);

# restart the subscriber
$node_subscriber->restart;

$node_publisher->safe_psql('postgres', "INSERT INTO tab_rep VALUES (11)");

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
  $node_subscriber->safe_psql('postgres', "SELECT sum(a) FROM tab_rep");
is($result, '21', 'replicated table contains data on subscriber');

$node_subscriber->stop('fast');
$node_publisher->stop('fast');
