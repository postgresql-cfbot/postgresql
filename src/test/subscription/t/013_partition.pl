# Test PARTITION
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 34;

# setup

my $node_publisher = get_new_node('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

my $node_subscriber1 = get_new_node('subscriber1');
$node_subscriber1->init(allows_streaming => 'logical');
$node_subscriber1->start;

my $node_subscriber2 = get_new_node('subscriber2');
$node_subscriber2->init(allows_streaming => 'logical');
$node_subscriber2->start;

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';

# publisher
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION pub1");
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION pub_all FOR ALL TABLES WITH (publish_using_root_schema = true)");
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION pub2");
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION pub3 WITH (publish_using_root_schema = true)");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab1 (a int PRIMARY KEY, b text) PARTITION BY LIST (a)");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab1_1 (b text, a int NOT NULL)");
$node_publisher->safe_psql('postgres',
	"ALTER TABLE tab1 ATTACH PARTITION tab1_1 FOR VALUES IN (1, 2, 3)");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab1_2 PARTITION OF tab1 FOR VALUES IN (5, 6)");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab2 (a int PRIMARY KEY, b text) PARTITION BY LIST (a)");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab2_1 (b text, a int NOT NULL)");
$node_publisher->safe_psql('postgres',
	"ALTER TABLE tab2 ATTACH PARTITION tab2_1 FOR VALUES IN (1, 2, 3)");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab2_2 PARTITION OF tab2 FOR VALUES IN (5, 6)");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab3 (a int PRIMARY KEY, b text) PARTITION BY LIST (a)");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab3_1 PARTITION OF tab3 FOR VALUES IN (1, 2, 3, 5, 6)");
$node_publisher->safe_psql('postgres',
	"ALTER PUBLICATION pub1 ADD TABLE tab1, tab1_1");
$node_publisher->safe_psql('postgres',
	"ALTER PUBLICATION pub2 ADD TABLE tab1_1, tab1_2");
$node_publisher->safe_psql('postgres',
	"ALTER PUBLICATION pub3 ADD TABLE tab2, tab3_1");

# subscriber1
$node_subscriber1->safe_psql('postgres',
	"CREATE TABLE tab1 (a int PRIMARY KEY, b text, c text) PARTITION BY LIST (a)");
$node_subscriber1->safe_psql('postgres',
	"CREATE TABLE tab1_1 (b text, c text DEFAULT 'sub1_tab1', a int NOT NULL)");

$node_subscriber1->safe_psql('postgres',
	"ALTER TABLE tab1 ATTACH PARTITION tab1_1 FOR VALUES IN (1, 2, 3, 4)");
$node_subscriber1->safe_psql('postgres',
	"CREATE TABLE tab1_2 PARTITION OF tab1 (c DEFAULT 'sub1_tab1') FOR VALUES IN (5, 6) PARTITION BY LIST (a)");
$node_subscriber1->safe_psql('postgres',
	"CREATE TABLE tab1_2_1 PARTITION OF tab1_2 FOR VALUES IN (5)");
$node_subscriber1->safe_psql('postgres',
	"CREATE TABLE tab1_2_2 PARTITION OF tab1_2 FOR VALUES IN (6)");
$node_subscriber1->safe_psql('postgres',
	"CREATE TABLE tab2 (a int PRIMARY KEY, c text DEFAULT 'sub1_tab2', b text) PARTITION BY RANGE (a)");
$node_subscriber1->safe_psql('postgres',
	"CREATE TABLE tab2_1 (c text DEFAULT 'sub1_tab2', b text, a int NOT NULL)");
$node_subscriber1->safe_psql('postgres',
	"CREATE TABLE tab3_1 (c text DEFAULT 'sub1_tab3_1', b text, a int NOT NULL PRIMARY KEY)");
$node_subscriber1->safe_psql('postgres',
	"ALTER TABLE tab2 ATTACH PARTITION tab2_1 FOR VALUES FROM (1) TO (10)");
$node_subscriber1->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub1 CONNECTION '$publisher_connstr' PUBLICATION pub1");
$node_subscriber1->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub4 CONNECTION '$publisher_connstr' PUBLICATION pub3");

# subscriber 2
$node_subscriber2->safe_psql('postgres',
	"CREATE TABLE tab1 (a int PRIMARY KEY, c text DEFAULT 'sub2_tab1', b text) PARTITION BY HASH (a)");
$node_subscriber2->safe_psql('postgres',
	"CREATE TABLE tab1_part1 (b text, c text, a int NOT NULL)");
$node_subscriber2->safe_psql('postgres',
	"ALTER TABLE tab1 ATTACH PARTITION tab1_part1 FOR VALUES WITH (MODULUS 2, REMAINDER 0)");
$node_subscriber2->safe_psql('postgres',
	"CREATE TABLE tab1_part2 PARTITION OF tab1 FOR VALUES WITH (MODULUS 2, REMAINDER 1)");
$node_subscriber2->safe_psql('postgres',
	"CREATE TABLE tab1_1 (a int PRIMARY KEY, c text DEFAULT 'sub2_tab1_1', b text)");
$node_subscriber2->safe_psql('postgres',
	"CREATE TABLE tab1_2 (a int PRIMARY KEY, c text DEFAULT 'sub2_tab1_2', b text)");
$node_subscriber2->safe_psql('postgres',
	"CREATE TABLE tab2 (a int PRIMARY KEY, c text DEFAULT 'sub2_tab2', b text)");
$node_subscriber2->safe_psql('postgres',
	"CREATE TABLE tab3 (a int PRIMARY KEY, c text DEFAULT 'sub2_tab1_1', b text)");
$node_subscriber2->safe_psql('postgres',
	"CREATE TABLE tab3_1 (a int PRIMARY KEY, c text DEFAULT 'sub2_tab1_2', b text)");
$node_subscriber2->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub2 CONNECTION '$publisher_connstr' PUBLICATION pub_all");
$node_subscriber2->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub3 CONNECTION '$publisher_connstr' PUBLICATION pub2");

# Wait for initial sync of all subscriptions
my $synced_query =
  "SELECT count(1) = 0 FROM pg_subscription_rel WHERE srsubstate NOT IN ('r', 's');";
$node_subscriber1->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";
$node_subscriber2->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

# insert
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab1 VALUES (1)");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab1_1 (a) VALUES (3)");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab1_2 VALUES (5)");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab2 VALUES (1), (3), (5)");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab3 VALUES (1), (3), (5)");

$node_publisher->wait_for_catchup('sub1');
$node_publisher->wait_for_catchup('sub2');
$node_publisher->wait_for_catchup('sub3');
$node_publisher->wait_for_catchup('sub4');

my $result = $node_subscriber1->safe_psql('postgres',
	"SELECT c, count(*), min(a), max(a) FROM tab1 GROUP BY 1");
is($result, qq(sub1_tab1|3|1|5), 'insert into tab1_1, tab1_2 replicated');

$result = $node_subscriber1->safe_psql('postgres',
	"SELECT c, count(*), min(a), max(a) FROM tab2 GROUP BY 1");
is($result, qq(sub1_tab2|3|1|5), 'insert into tab2 replicated');

$result = $node_subscriber1->safe_psql('postgres',
	"SELECT c, count(*), min(a), max(a) FROM tab3_1 GROUP BY 1");
is($result, qq(sub1_tab3_1|3|1|5), 'insert into tab3_1 replicated');

$result = $node_subscriber2->safe_psql('postgres',
	"SELECT c, count(*), min(a), max(a) FROM tab1_1 GROUP BY 1");
is($result, qq(sub2_tab1_1|2|1|3), 'inserts into tab1_1 replicated');

$result = $node_subscriber2->safe_psql('postgres',
	"SELECT c, count(*), min(a), max(a) FROM tab1_2 GROUP BY 1");
is($result, qq(sub2_tab1_2|1|5|5), 'inserts into tab1_2 replicated');

$result = $node_subscriber2->safe_psql('postgres',
	"SELECT c, count(*), min(a), max(a) FROM tab1 GROUP BY 1");
is($result, qq(sub2_tab1|3|1|5), 'inserts into tab1 replicated');

# update (no partition change)
$node_publisher->safe_psql('postgres',
	"UPDATE tab1 SET a = 2 WHERE a = 1");
$node_publisher->safe_psql('postgres',
	"UPDATE tab2 SET a = 2 WHERE a = 1");
$node_publisher->safe_psql('postgres',
	"UPDATE tab3 SET a = 2 WHERE a = 1");

$node_publisher->wait_for_catchup('sub1');
$node_publisher->wait_for_catchup('sub2');
$node_publisher->wait_for_catchup('sub3');
$node_publisher->wait_for_catchup('sub4');

$result = $node_subscriber1->safe_psql('postgres',
	"SELECT c, count(*), min(a), max(a) FROM tab1 GROUP BY 1");
is($result, qq(sub1_tab1|3|2|5), 'update of tab1_1 replicated');

$result = $node_subscriber1->safe_psql('postgres',
	"SELECT c, count(*), min(a), max(a) FROM tab2 GROUP BY 1");
is($result, qq(sub1_tab2|3|2|5), 'update of tab2 replicated');

$result = $node_subscriber1->safe_psql('postgres',
	"SELECT c, count(*), min(a), max(a) FROM tab3_1 GROUP BY 1");
is($result, qq(sub1_tab3_1|3|2|5), 'update of tab3_1 replicated');

$result = $node_subscriber2->safe_psql('postgres',
	"SELECT c, count(*), min(a), max(a) FROM tab1_1 GROUP BY 1");
is($result, qq(sub2_tab1_1|2|2|3), 'update of tab1_1 replicated');

$result = $node_subscriber2->safe_psql('postgres',
	"SELECT c, count(*), min(a), max(a) FROM tab1 GROUP BY 1");
is($result, qq(sub2_tab1|3|2|5), 'update of tab1 replicated');

# update (partition changes)
$node_publisher->safe_psql('postgres',
	"UPDATE tab1 SET a = 6 WHERE a = 2");
$node_publisher->safe_psql('postgres',
	"UPDATE tab2 SET a = 6 WHERE a = 2");
$node_publisher->safe_psql('postgres',
	"UPDATE tab3 SET a = 6 WHERE a = 2");

$node_publisher->wait_for_catchup('sub1');
$node_publisher->wait_for_catchup('sub2');
$node_publisher->wait_for_catchup('sub3');
$node_publisher->wait_for_catchup('sub4');

$result = $node_subscriber1->safe_psql('postgres',
	"SELECT c, count(*), min(a), max(a) FROM tab1 GROUP BY 1");
is($result, qq(sub1_tab1|3|3|6), 'update of tab1 replicated');

$result = $node_subscriber1->safe_psql('postgres',
	"SELECT c, count(*), min(a), max(a) FROM tab2 GROUP BY 1");
is($result, qq(sub1_tab2|3|3|6), 'update of tab2 replicated');

$result = $node_subscriber1->safe_psql('postgres',
	"SELECT c, count(*), min(a), max(a) FROM tab3_1 GROUP BY 1");
is($result, qq(sub1_tab3_1|3|3|6), 'update of tab3_1 replicated');

$result = $node_subscriber2->safe_psql('postgres',
	"SELECT c, count(*), min(a), max(a) FROM tab1_1 GROUP BY 1");
is($result, qq(sub2_tab1_1|1|3|3), 'delete from tab1_1 replicated');

$result = $node_subscriber2->safe_psql('postgres',
	"SELECT c, count(*), min(a), max(a) FROM tab1_2 GROUP BY 1");
is($result, qq(sub2_tab1_2|2|5|6), 'insert into tab1_2 replicated');

$result = $node_subscriber2->safe_psql('postgres',
	"SELECT c, count(*), min(a), max(a) FROM tab1_1 GROUP BY 1");
is($result, qq(sub2_tab1_1|1|3|3), 'delete from tab1_1 replicated');

$result = $node_subscriber2->safe_psql('postgres',
	"SELECT c, count(*), min(a), max(a) FROM tab1 GROUP BY 1");
is($result, qq(sub2_tab1|3|3|6), 'update of tab1 replicated');

# delete
$node_publisher->safe_psql('postgres',
	"DELETE FROM tab1 WHERE a IN (3, 5)");
$node_publisher->safe_psql('postgres',
	"DELETE FROM tab1_2");
$node_publisher->safe_psql('postgres',
	"DELETE FROM tab2 WHERE a IN (3, 5)");
$node_publisher->safe_psql('postgres',
	"DELETE FROM tab3 WHERE a IN (3, 5)");

$node_publisher->wait_for_catchup('sub1');
$node_publisher->wait_for_catchup('sub2');
$node_publisher->wait_for_catchup('sub3');
$node_publisher->wait_for_catchup('sub4');

$result = $node_subscriber1->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab1");
is($result, qq(0||), 'delete from tab1_1, tab1_2 replicated');

$result = $node_subscriber1->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab2");
is($result, qq(1|6|6), 'delete from tab2 replicated');

$result = $node_subscriber1->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab3_1");
is($result, qq(1|6|6), 'delete from tab3_1 replicated');

$result = $node_subscriber2->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab1_1");
is($result, qq(0||), 'delete from tab1_1 replicated');

$result = $node_subscriber2->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab1_2");
is($result, qq(0||), 'delete from tab1_2 replicated');

$result = $node_subscriber2->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab1_1");
is($result, qq(0||), 'delete from tab1_1, tab_2 replicated');

$result = $node_subscriber2->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab1");
is($result, qq(0||), 'delete from tab1 replicated');

# truncate
$node_subscriber1->safe_psql('postgres',
	"INSERT INTO tab1 VALUES (1), (2), (5)");
$node_subscriber1->safe_psql('postgres',
	"INSERT INTO tab2 VALUES (1), (2), (5)");
$node_subscriber1->safe_psql('postgres',
	"INSERT INTO tab3_1 (a) VALUES (1), (2), (5)");
$node_subscriber2->safe_psql('postgres',
	"INSERT INTO tab1_2 VALUES (2)");
$node_subscriber2->safe_psql('postgres',
	"INSERT INTO tab1_1 VALUES (1)");
$node_subscriber2->safe_psql('postgres',
	"INSERT INTO tab1 VALUES (1), (2), (5)");

$node_publisher->safe_psql('postgres',
	"TRUNCATE tab1_2");
$node_publisher->safe_psql('postgres',
	"TRUNCATE tab2_1");

$node_publisher->wait_for_catchup('sub1');
$node_publisher->wait_for_catchup('sub2');
$node_publisher->wait_for_catchup('sub3');
$node_publisher->wait_for_catchup('sub4');

$result = $node_subscriber1->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab1");
is($result, qq(2|1|2), 'truncate of tab1_2 replicated');

$result = $node_subscriber1->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab2");
is($result, qq(4|1|6), 'truncate of tab2_2 NOT replicated');

$result = $node_subscriber2->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab1_2");
is($result, qq(0||), 'truncate of tab1_2 replicated');

$node_subscriber2->safe_psql('postgres',
	"DROP SUBSCRIPTION sub3");
$node_subscriber2->safe_psql('postgres',
	"INSERT INTO tab1_2 VALUES (2)");
$node_publisher->safe_psql('postgres',
	"TRUNCATE tab1");
$node_publisher->safe_psql('postgres',
	"TRUNCATE tab2");
$node_publisher->safe_psql('postgres',
	"TRUNCATE tab3");

$node_publisher->wait_for_catchup('sub1');
$node_publisher->wait_for_catchup('sub2');
$node_publisher->wait_for_catchup('sub4');

$result = $node_subscriber1->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab1");
is($result, qq(0||), 'truncate of tab1_1 replicated');
$result = $node_subscriber2->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab1");
is($result, qq(0||), 'truncate of tab1 replicated');
$result = $node_subscriber2->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab1_1");
is($result, qq(1|1|1), 'tab1_1 unchanged');
$result = $node_subscriber1->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab2");
is($result, qq(0||), 'truncate of tab2 replicated');
$result = $node_subscriber1->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab3_1");
is($result, qq(0||), 'truncate of tab3_1 replicated');
$result = $node_subscriber2->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab1_2");
is($result, qq(1|2|2), 'tab1_2 unchanged');
