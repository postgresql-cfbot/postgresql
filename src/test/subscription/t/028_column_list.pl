# Copyright (c) 2022, PostgreSQL Global Development Group

# Test partial-column publication of tables
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 9;

# setup

my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->append_conf('postgresql.conf',
	qq(max_logical_replication_workers = 6));
$node_subscriber->start;

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';

$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab1 (a int PRIMARY KEY, \"B\" int, c int)");

$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab1 (a int PRIMARY KEY, \"B\" int, c int)");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab2 (a int PRIMARY KEY, b varchar, c int);
	INSERT INTO tab2 VALUES (2, 'foo', 2);");
# Test with weird column names
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab3 (\"a'\" int PRIMARY KEY, B varchar, \"c'\" int)");

$node_publisher->safe_psql('postgres',
	"CREATE TABLE test_part (a int PRIMARY KEY, b text, c timestamptz) PARTITION BY LIST (a)");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE test_part_1_1 PARTITION OF test_part FOR VALUES IN (1,2,3)");
# Test replication with multi-level partition
$node_publisher->safe_psql('postgres',
	"CREATE TABLE test_part_2_1 PARTITION OF test_part FOR VALUES IN (4,5,6) PARTITION BY LIST (a)");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE test_part_2_2 PARTITION OF test_part_2_1 FOR VALUES IN (4,5)");

$node_subscriber->safe_psql('postgres',
	"CREATE TABLE test_part (a int PRIMARY KEY, b text) PARTITION BY LIST (a)");
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE test_part_1_1 PARTITION OF test_part FOR VALUES IN (1,2,3)");
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab3 (\"a'\" int PRIMARY KEY, \"c'\" int)");
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab2 (a int PRIMARY KEY, b varchar)");
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE test_part_2_1 PARTITION OF test_part FOR VALUES IN (4,5,6) PARTITION BY LIST (a)");
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE test_part_2_2 PARTITION OF test_part_2_1 FOR VALUES IN (4,5)");

# Test create publication with a column list
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION pub1 FOR TABLE tab1(a, \"B\"), tab3(\"a'\",\"c'\"), test_part(a,b)");

my $result = $node_publisher->safe_psql('postgres',
	"select relname, prattrs from pg_publication_rel pb, pg_class pc where pb.prrelid = pc.oid;");
is($result, qq(tab1|1 2
tab3|1 3
test_part|1 2), 'publication relation updated');

$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub1 CONNECTION '$publisher_connstr' PUBLICATION pub1"
);
# Initial sync
$node_publisher->wait_for_catchup('sub1');

$node_publisher->safe_psql('postgres',
	"INSERT INTO tab1 VALUES (1,2,3)");

$node_publisher->safe_psql('postgres',
	"INSERT INTO tab3 VALUES (1,2,3)");
# Test for replication of partition data
$node_publisher->safe_psql('postgres',
	"INSERT INTO test_part VALUES (1,'abc', '2021-07-04 12:00:00')");
$node_publisher->safe_psql('postgres',
	"INSERT INTO test_part VALUES (2,'bcd', '2021-07-03 11:12:13')");
# Test for replication of multi-level partition data
$node_publisher->safe_psql('postgres',
	"INSERT INTO test_part VALUES (4,'abc', '2021-07-04 12:00:00')");
$node_publisher->safe_psql('postgres',
	"INSERT INTO test_part VALUES (5,'bcd', '2021-07-03 11:12:13')");

$result = $node_subscriber->safe_psql('postgres',
	"SELECT * FROM tab1");
is($result, qq(1|2|), 'insert on column tab1.c is not replicated');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT * FROM tab3");
is($result, qq(1|3), 'insert on column tab3.b is not replicated');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT * FROM test_part");
is($result, qq(1|abc\n2|bcd\n4|abc\n5|bcd), 'insert on all columns is replicated');

$node_publisher->safe_psql('postgres',
	"UPDATE tab1 SET c = 5 where a = 1");

$node_publisher->wait_for_catchup('sub1');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT * FROM tab1");
is($result, qq(1|2|), 'update on column tab1.c is not replicated');

# Verify user-defined types
$node_publisher->safe_psql('postgres',
	qq{CREATE TYPE test_typ AS ENUM ('blue', 'red');
	CREATE TABLE test_tab4 (a INT PRIMARY KEY, b test_typ, c int, d text);
	ALTER PUBLICATION pub1 ADD TABLE test_tab4 (a, b, d);
	});
$node_subscriber->safe_psql('postgres',
	qq{CREATE TYPE test_typ AS ENUM ('blue', 'red');
	CREATE TABLE test_tab4 (a INT PRIMARY KEY, b test_typ, d text);
	});
$node_publisher->safe_psql('postgres',
	"INSERT INTO test_tab4 VALUES (1, 'red', 3, 'oh my');");

# Test alter publication with a column list
$node_publisher->safe_psql('postgres',
	"ALTER PUBLICATION pub1 ADD TABLE tab2(a, b)");

$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION sub1 REFRESH PUBLICATION"
);

$node_publisher->safe_psql('postgres',
	"INSERT INTO tab2 VALUES (1,'abc',3)");
$node_publisher->safe_psql('postgres',
	"UPDATE tab2 SET c = 5 where a = 2");

$node_publisher->wait_for_catchup('sub1');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT * FROM tab2 WHERE a = 1");
is($result, qq(1|abc), 'insert on column tab2.c is not replicated');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT * FROM tab2 WHERE a = 2");
is($result, qq(2|foo), 'update on column tab2.c is not replicated');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT * FROM test_tab4");
is($result, qq(1|red|oh my), 'insert on table with user-defined type');

$node_publisher->safe_psql('postgres', "CREATE TABLE tab5 (a int PRIMARY KEY, b int, c int, d int)");
$node_subscriber->safe_psql('postgres', "CREATE TABLE tab5 (a int PRIMARY KEY, b int, d int)");
$node_publisher->safe_psql('postgres', "CREATE PUBLICATION pub2 FOR TABLE tab5 (a, b)");
$node_publisher->safe_psql('postgres', "CREATE PUBLICATION pub3 FOR TABLE tab5 (a, d)");
$node_subscriber->safe_psql('postgres',    "CREATE SUBSCRIPTION sub2 CONNECTION '$publisher_connstr' PUBLICATION pub2, pub3");
$node_publisher->wait_for_catchup('sub2');
$node_publisher->safe_psql('postgres', "INSERT INTO tab5 VALUES (1, 11, 111, 1111)");
$node_publisher->safe_psql('postgres', "INSERT INTO tab5 VALUES (2, 22, 222, 2222)");
$node_publisher->wait_for_catchup('sub2');
is($node_subscriber->safe_psql('postgres',"SELECT * FROM tab5;"),
   qq(1|11|1111
2|22|2222),
   'overlapping publications with overlapping column lists');
