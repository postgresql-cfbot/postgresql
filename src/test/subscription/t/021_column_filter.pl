# Copyright (c) 2021, PostgreSQL Global Development Group

# Test TRUNCATE
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 3;

# setup

my $node_publisher = get_new_node('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

my $node_subscriber = get_new_node('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->append_conf('postgresql.conf',
	qq(max_logical_replication_workers = 6));
$node_subscriber->start;

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';

$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab1 (a int PRIMARY KEY, b int, c int)");

$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab1 (a int PRIMARY KEY, b int, c int)");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab2 (a int PRIMARY KEY, b varchar, c int)");
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab2 (a int PRIMARY KEY, b varchar, c int)");

#Test create publication with column filtering
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION pub1 FOR TABLE tab1(a, b)");

$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub1 CONNECTION '$publisher_connstr' PUBLICATION pub1"
);
#Initial sync
$node_publisher->wait_for_catchup('sub1');

$node_publisher->safe_psql('postgres',
	"INSERT INTO tab1 VALUES (1,2,3)");

my $result = $node_subscriber->safe_psql('postgres',
	"SELECT * FROM tab1");
is($result, qq(1|2|), 'insert on column c is not replicated');

$node_publisher->safe_psql('postgres',
	"UPDATE tab1 SET c = 5 where a = 1");

$node_publisher->wait_for_catchup('sub1');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT * FROM tab1");
is($result, qq(1|2|), 'update on column c is not replicated');

#Test alter publication with column filtering
$node_publisher->safe_psql('postgres',
	"ALTER PUBLICATION pub1 ADD TABLE tab2(a, b)");

$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION sub1 REFRESH PUBLICATION"
);

$node_publisher->wait_for_catchup('sub1');
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab2 VALUES (1,'abc',3)");

$node_publisher->wait_for_catchup('sub1');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT * FROM tab2");
is($result, qq(1|abc|), 'insert on column c is not replicated');
