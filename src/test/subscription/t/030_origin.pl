
# Copyright (c) 2021-2022, PostgreSQL Global Development Group

# Test the CREATE SUBSCRIPTION 'origin' parameter and its interaction with
# 'copy_data' parameter.
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $result;
my $stdout;
my $stderr;

my $subname_AB = 'tap_sub_A_B';
my $subname_AC = 'tap_sub_A_C';
my $subname_BA = 'tap_sub_B_A';
my $subname_BC = 'tap_sub_B_C';
my $subname_CA = 'tap_sub_C_A';
my $subname_CB = 'tap_sub_C_B';

# Detach node_C from the node-group of (node_A, node_B, node_C) and clean the
# table contents from all nodes.
sub detach_node_clean_table_data
{
	my ($node_A, $node_B, $node_C) = @_;
	$node_A->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub_A_C");
	$node_B->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub_B_C");
	$node_C->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub_C_A");
	$node_C->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub_C_B");

	$result =
	  $node_A->safe_psql('postgres', "SELECT count(*) FROM pg_subscription");
	is($result, qq(1), 'check subscription was dropped on subscriber');

	$result =
	  $node_B->safe_psql('postgres', "SELECT count(*) FROM pg_subscription");
	is($result, qq(1), 'check subscription was dropped on subscriber');

	$result =
	  $node_C->safe_psql('postgres', "SELECT count(*) FROM pg_subscription");
	is($result, qq(0), 'check subscription was dropped on subscriber');

	$result = $node_A->safe_psql('postgres',
		"SELECT count(*) FROM pg_replication_slots");
	is($result, qq(1), 'check replication slot was dropped on publisher');

	$result = $node_B->safe_psql('postgres',
		"SELECT count(*) FROM pg_replication_slots");
	is($result, qq(1), 'check replication slot was dropped on publisher');

	$result = $node_C->safe_psql('postgres',
		"SELECT count(*) FROM pg_replication_slots");
	is($result, qq(0), 'check replication slot was dropped on publisher');

	$node_A->safe_psql('postgres', "TRUNCATE tab");
	$node_B->safe_psql('postgres', "TRUNCATE tab");
	$node_C->safe_psql('postgres', "TRUNCATE tab");
}

# Subroutine to verify the data is replicated successfully.
sub verify_data
{
	my ($node_A, $node_B, $node_C, $expect) = @_;

	$node_A->wait_for_catchup($subname_BA);
	$node_A->wait_for_catchup($subname_CA);
	$node_B->wait_for_catchup($subname_AB);
	$node_B->wait_for_catchup($subname_CB);
	$node_C->wait_for_catchup($subname_AC);
	$node_C->wait_for_catchup($subname_BC);

	# check that data is replicated to all the nodes
	$result = $node_A->safe_psql('postgres', "SELECT * FROM tab ORDER BY 1;");
	is($result, qq($expect), 'Data is replicated as expected');

	$result = $node_B->safe_psql('postgres', "SELECT * FROM tab ORDER BY 1;");
	is($result, qq($expect), 'Data is replicated as expected');

	$result = $node_C->safe_psql('postgres', "SELECT * FROM tab ORDER BY 1;");
	is($result, qq($expect), 'Data is replicated as expected');
}

my $synced_query =
  "SELECT count(1) = 0 FROM pg_subscription_rel WHERE srsubstate NOT IN ('r', 's');";

# Subroutine to create subscription and wait until the initial sync is
# completed. Subroutine expects subscriber node, publisher node, subscription
# name, destination connection string, publication name and the subscription
# parameters to be passed as input parameters.
sub create_subscription
{
	my ($node_subscriber, $node_publisher, $sub_name, $node_connstr,
		$pub_name, $sub_params)
	  = @_;

	# Application_name is always assigned the same value as the subscription
	# name.
	$node_subscriber->safe_psql(
		'postgres', "
                CREATE SUBSCRIPTION $sub_name
                CONNECTION '$node_connstr application_name=$sub_name'
                PUBLICATION $pub_name
                WITH ($sub_params)");
	$node_publisher->wait_for_catchup($sub_name);

	# also wait for initial table sync to finish
	$node_subscriber->poll_query_until('postgres', $synced_query)
	  or die "Timed out while waiting for subscriber to synchronize data";
}

###############################################################################
# Setup a bidirectional logical replication between node_A & node_B
###############################################################################

# Initialize nodes
# node_A
my $node_A = PostgreSQL::Test::Cluster->new('node_A');
$node_A->init(allows_streaming => 'logical');
$node_A->start;
# node_B
my $node_B = PostgreSQL::Test::Cluster->new('node_B');
$node_B->init(allows_streaming => 'logical');
$node_B->start;

# Create table on node_A
$node_A->safe_psql('postgres', "CREATE TABLE tab (a int PRIMARY KEY)");

# Create the same table on node_B
$node_B->safe_psql('postgres', "CREATE TABLE tab (a int PRIMARY KEY)");

# Setup logical replication
# node_A (pub) -> node_B (sub)
my $node_A_connstr = $node_A->connstr . ' dbname=postgres';
$node_A->safe_psql('postgres', "CREATE PUBLICATION tap_pub_A FOR TABLE tab");
create_subscription($node_B, $node_A, $subname_BA, $node_A_connstr,
	'tap_pub_A', 'copy_data = on, origin = none');

# node_B (pub) -> node_A (sub)
my $node_B_connstr = $node_B->connstr . ' dbname=postgres';
$node_B->safe_psql('postgres', "CREATE PUBLICATION tap_pub_B FOR TABLE tab");
create_subscription($node_A, $node_B, $subname_AB, $node_B_connstr,
	'tap_pub_B', 'copy_data = off, origin = none');

is(1, 1, 'Bidirectional replication setup is complete');

###############################################################################
# Check that bidirectional logical replication setup does not cause infinite
# recursive insertion.
###############################################################################

# insert a record
$node_A->safe_psql('postgres', "INSERT INTO tab VALUES (11);");
$node_B->safe_psql('postgres', "INSERT INTO tab VALUES (21);");

$node_A->wait_for_catchup($subname_BA);
$node_B->wait_for_catchup($subname_AB);

# check that transaction was committed on subscriber(s)
$result = $node_A->safe_psql('postgres', "SELECT * FROM tab ORDER BY 1;");
is( $result, qq(11
21),
	'Inserted successfully without leading to infinite recursion in bidirectional replication setup'
);
$result = $node_B->safe_psql('postgres', "SELECT * FROM tab ORDER BY 1;");
is( $result, qq(11
21),
	'Inserted successfully without leading to infinite recursion in bidirectional replication setup'
);

$node_A->safe_psql('postgres', "DELETE FROM tab;");

$node_A->wait_for_catchup($subname_BA);
$node_B->wait_for_catchup($subname_AB);

###############################################################################
# Check that remote data of node_B (that originated from node_C) is not
# published to node_A.
###############################################################################
$result = $node_A->safe_psql('postgres', "SELECT * FROM tab ORDER BY 1;");
is($result, qq(), 'Check existing data');

$result = $node_B->safe_psql('postgres', "SELECT * FROM tab ORDER BY 1;");
is($result, qq(), 'Check existing data');

# Initialize node node_C
my $node_C = PostgreSQL::Test::Cluster->new('node_C');
$node_C->init(allows_streaming => 'logical');
$node_C->start;

$node_C->safe_psql('postgres', "CREATE TABLE tab (a int PRIMARY KEY)");

# Setup logical replication
# node_C (pub) -> node_B (sub)
my $node_C_connstr = $node_C->connstr . ' dbname=postgres';
$node_C->safe_psql('postgres', "CREATE PUBLICATION tap_pub_C FOR TABLE tab");
create_subscription($node_B, $node_C, $subname_BC, $node_C_connstr,
	'tap_pub_C', 'copy_data = on, origin = none');

# insert a record
$node_C->safe_psql('postgres', "INSERT INTO tab VALUES (32);");

$node_C->wait_for_catchup($subname_BC);
$node_B->wait_for_catchup($subname_AB);
$node_A->wait_for_catchup($subname_BA);

$result = $node_B->safe_psql('postgres', "SELECT * FROM tab ORDER BY 1;");
is($result, qq(32), 'The node_C data replicated to node_B');

# check that the data published from node_C to node_B is not sent to node_A
$result = $node_A->safe_psql('postgres', "SELECT * FROM tab ORDER BY 1;");
is($result, qq(),
	'Remote data originating from another node (not the publisher) is not replicated when origin parameter is none'
);

# clear the operations done by this test
$node_B->safe_psql(
	'postgres', "
        DROP SUBSCRIPTION $subname_BC");
# no need to wait for catchup of delete operation performed in node_C as
# the subscription for node_C publication has been dropped
$node_C->safe_psql(
	'postgres', "
        DELETE FROM tab");

# wait for catchup of bidirectional logical replication nodes node_A & node_B
$node_B->safe_psql(
	'postgres', "
        DELETE FROM tab where a = 32");

$node_A->wait_for_catchup($subname_BA);
$node_B->wait_for_catchup($subname_AB);

###############################################################################
# Specify origin as 'none' which indicates that the publisher should only
# replicate the changes that are generated locally from node_B, but in
# this case since the node_B is also subscribing data from node_A, node_B can
# have remotely originated data from node_A. We throw an error, in this case,
# to draw attention to there being possible remote data.
###############################################################################
($result, $stdout, $stderr) = $node_A->psql(
	'postgres', "
        CREATE SUBSCRIPTION tap_sub_A2
        CONNECTION '$node_B_connstr application_name=$subname_AB'
        PUBLICATION tap_pub_B
        WITH (origin = none, copy_data = on)");
like(
	$stderr,
	qr/ERROR: ( [A-Z0-9]+:)? could not replicate table "public.tab"/,
	"Create subscription with origin = none and copy_data when the publisher has subscribed same table"
);

# Creating subscription with origin as none and copy_data as force should be
# successful when the publisher has replicated data
$node_A->safe_psql(
	'postgres', "
        CREATE SUBSCRIPTION tap_sub_A2
        CONNECTION '$node_B_connstr application_name=$subname_AC'
        PUBLICATION tap_pub_B
        WITH (origin = none, copy_data = force)");

$node_B->wait_for_catchup($subname_AC);

# also wait for initial table sync to finish
$node_A->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

# Alter subscription ... refresh publication should be successful when no new
# table is added
$node_A->safe_psql(
	'postgres', "
        ALTER SUBSCRIPTION tap_sub_A2 REFRESH PUBLICATION");

# Check Alter subscription ... refresh publication when there is a new
# table that is subscribing data from a different publication
$node_A->safe_psql('postgres', "CREATE TABLE tab_new (a int PRIMARY KEY)");
$node_B->safe_psql('postgres', "CREATE TABLE tab_new (a int PRIMARY KEY)");

# add a new table to the publication
$node_A->safe_psql('postgres',
	"ALTER PUBLICATION tap_pub_A ADD TABLE tab_new");
$node_B->safe_psql('postgres',
	"ALTER PUBLICATION tap_pub_B ADD TABLE tab_new");

$node_B->safe_psql(
	'postgres', "
        ALTER SUBSCRIPTION $subname_BA REFRESH PUBLICATION");

$node_B->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

# Alter subscription ... refresh publication should fail when a new table in
# the publisher is subscribing data from a different publication
($result, $stdout, $stderr) = $node_A->psql(
	'postgres', "
        ALTER SUBSCRIPTION tap_sub_A2 REFRESH PUBLICATION");
like(
	$stderr,
	qr/ERROR: ( [A-Z0-9]+:)? could not replicate table "public.tab_new"/,
	"Refresh publication when the publisher has subscribed for the new table, but the subscriber-side wants origin=none"
);

# clear the operations done by this test
$node_A->safe_psql('postgres', "DROP TABLE tab_new");
$node_B->safe_psql('postgres', "DROP TABLE tab_new");
$node_A->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub_A2");
$node_A->safe_psql(
	'postgres', "
        ALTER SUBSCRIPTION $subname_AB REFRESH PUBLICATION");
$node_A->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";
$node_B->safe_psql(
	'postgres', "
        ALTER SUBSCRIPTION $subname_BA REFRESH PUBLICATION");
$node_B->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

###############################################################################
# Join 3rd node (node_C) to the existing 2 nodes(node_A & node_B) bidirectional
# replication setup when the existing nodes (node_A & node_B) has pre-existing
# data and the new node (node_C) does not have any data.
###############################################################################
$result = $node_A->safe_psql('postgres', "SELECT * FROM tab ORDER BY 1;");
is($result, qq(), 'Check existing data');

$result = $node_B->safe_psql('postgres', "SELECT * FROM tab ORDER BY 1;");
is($result, qq(), 'Check existing data');

$result = $node_C->safe_psql('postgres', "SELECT * FROM tab ORDER BY 1;");
is($result, qq(), 'Check existing data');

create_subscription($node_A, $node_C, $subname_AC, $node_C_connstr,
	'tap_pub_C', 'copy_data = off, origin = none');
create_subscription($node_B, $node_C, $subname_BC, $node_C_connstr,
	'tap_pub_C', 'copy_data = off, origin = none');
create_subscription($node_C, $node_A, $subname_CA, $node_A_connstr,
	'tap_pub_A', 'copy_data = force, origin = none');
create_subscription($node_C, $node_B, $subname_CB, $node_B_connstr,
	'tap_pub_B', 'copy_data = off, origin = none');

# insert some data in all the nodes
$node_A->safe_psql('postgres', "INSERT INTO tab VALUES (13);");
$node_B->safe_psql('postgres', "INSERT INTO tab VALUES (23);");
$node_C->safe_psql('postgres', "INSERT INTO tab VALUES (33);");

verify_data(
	$node_A, $node_B, $node_C, '13
23
33');

detach_node_clean_table_data($node_A, $node_B, $node_C);

###############################################################################
# Join 3rd node (node_C) to the existing 2 nodes(node_A & node_B) bidirectional
# replication setup when the existing nodes (node_A & node_B) and the new node
# (node_C) does not have any data.
###############################################################################
$result = $node_A->safe_psql('postgres', "SELECT * FROM tab ORDER BY 1;");
is($result, qq(), 'Check existing data');

$result = $node_B->safe_psql('postgres', "SELECT * FROM tab ORDER BY 1;");
is($result, qq(), 'Check existing data');

$result = $node_C->safe_psql('postgres', "SELECT * FROM tab ORDER BY 1;");
is($result, qq(), 'Check existing data');

create_subscription($node_A, $node_C, $subname_AC, $node_C_connstr,
	'tap_pub_C', 'copy_data = off, origin = none');
create_subscription($node_B, $node_C, $subname_BC, $node_C_connstr,
	'tap_pub_C', 'copy_data = off, origin = none');
create_subscription($node_C, $node_A, $subname_CA, $node_A_connstr,
	'tap_pub_A', 'copy_data = off, origin = none');
create_subscription($node_C, $node_B, $subname_CB, $node_B_connstr,
	'tap_pub_B', 'copy_data = off, origin = none');

# insert some data in all the nodes
$node_A->safe_psql('postgres', "INSERT INTO tab VALUES (14);");
$node_B->safe_psql('postgres', "INSERT INTO tab VALUES (24);");
$node_C->safe_psql('postgres', "INSERT INTO tab VALUES (34);");

verify_data(
	$node_A, $node_B, $node_C, '14
24
34');

detach_node_clean_table_data($node_A, $node_B, $node_C);

###############################################################################
# Join 3rd node (node_C) to the existing 2 nodes(node_A & node_B) bidirectional
# replication setup when the existing nodes (node_A & node_B) has no data and
# the new node (node_C) some pre-existing data.
###############################################################################
$node_C->safe_psql('postgres', "INSERT INTO tab VALUES (35);");

$result = $node_A->safe_psql('postgres', "SELECT * FROM tab ORDER BY 1;");
is($result, qq(), 'Check existing data');

$result = $node_B->safe_psql('postgres', "SELECT * FROM tab ORDER BY 1;");
is($result, qq(), 'Check existing data');

$result = $node_C->safe_psql('postgres', "SELECT * FROM tab ORDER BY 1;");
is($result, qq(35), 'Check existing data');

create_subscription($node_A, $node_C, $subname_AC, $node_C_connstr,
	'tap_pub_C', 'copy_data = on, origin = none');
create_subscription($node_B, $node_C, $subname_BC, $node_C_connstr,
	'tap_pub_C', 'copy_data = on, origin = none');

$node_C->safe_psql('postgres',
	"ALTER PUBLICATION tap_pub_C SET (publish='insert,update,delete');");

$node_C->safe_psql('postgres', "TRUNCATE tab");

# include truncates now
$node_C->safe_psql('postgres',
	"ALTER PUBLICATION tap_pub_C SET (publish='insert,update,delete,truncate');"
);

create_subscription($node_C, $node_A, $subname_CA, $node_A_connstr,
	'tap_pub_A', 'copy_data = force, origin = none');
create_subscription($node_C, $node_B, $subname_CB, $node_B_connstr,
	'tap_pub_B', 'copy_data = off, origin = none');

# insert some data in all the nodes
$node_A->safe_psql('postgres', "INSERT INTO tab VALUES (16);");
$node_B->safe_psql('postgres', "INSERT INTO tab VALUES (26);");
$node_C->safe_psql('postgres', "INSERT INTO tab VALUES (36);");

verify_data(
	$node_A, $node_B, $node_C, '16
26
35
36');

# shutdown
$node_B->stop('fast');
$node_A->stop('fast');
$node_C->stop('fast');

done_testing();
