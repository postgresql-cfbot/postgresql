# Test logical replication behavior with row filtering
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 7;

# create publisher node
my $node_publisher = get_new_node('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

# create subscriber node
my $node_subscriber = get_new_node('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->start;

# setup structure on publisher
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab_rowfilter_1 (a int primary key, b text)");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab_rowfilter_2 (c int primary key)");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab_rowfilter_3 (a int primary key, b boolean)");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab_rowfilter_partitioned (a int primary key, b integer) PARTITION BY RANGE(a)"
);
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab_rowfilter_less_10k (LIKE tab_rowfilter_partitioned)");
$node_publisher->safe_psql('postgres',
	"ALTER TABLE tab_rowfilter_partitioned ATTACH PARTITION tab_rowfilter_less_10k FOR VALUES FROM (MINVALUE) TO (10000)"
);
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab_rowfilter_greater_10k (LIKE tab_rowfilter_partitioned)"
);
$node_publisher->safe_psql('postgres',
	"ALTER TABLE tab_rowfilter_partitioned ATTACH PARTITION tab_rowfilter_greater_10k FOR VALUES FROM (10000) TO (MAXVALUE)"
);

# setup structure on subscriber
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab_rowfilter_1 (a int primary key, b text)");
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab_rowfilter_2 (c int primary key)");
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab_rowfilter_3 (a int primary key, b boolean)");
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab_rowfilter_partitioned (a int primary key, b integer) PARTITION BY RANGE(a)"
);
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab_rowfilter_less_10k (LIKE tab_rowfilter_partitioned)");
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE tab_rowfilter_partitioned ATTACH PARTITION tab_rowfilter_less_10k FOR VALUES FROM (MINVALUE) TO (10000)"
);
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab_rowfilter_greater_10k (LIKE tab_rowfilter_partitioned)"
);
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE tab_rowfilter_partitioned ATTACH PARTITION tab_rowfilter_greater_10k FOR VALUES FROM (10000) TO (MAXVALUE)"
);

# setup logical replication
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub_1 FOR TABLE tab_rowfilter_1 WHERE (a > 1000 AND b <> 'filtered')"
);

$node_publisher->safe_psql('postgres',
	"ALTER PUBLICATION tap_pub_1 ADD TABLE tab_rowfilter_2 WHERE (c % 7 = 0)"
);

$node_publisher->safe_psql('postgres',
	"ALTER PUBLICATION tap_pub_1 SET TABLE tab_rowfilter_1 WHERE (a > 1000 AND b <> 'filtered'), tab_rowfilter_2 WHERE (c % 2 = 0), tab_rowfilter_3"
);

$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub_2 FOR TABLE tab_rowfilter_2 WHERE (c % 3 = 0)"
);

$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub_3 FOR TABLE tab_rowfilter_partitioned WHERE (a < 5000)"
);
$node_publisher->safe_psql('postgres',
	"ALTER PUBLICATION tap_pub_3 ADD TABLE tab_rowfilter_less_10k WHERE (a < 6000)"
);
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub_not_used FOR TABLE tab_rowfilter_1 WHERE (a < 0)"
);

#
# The following INSERTs are executed before the CREATE SUBSCRIPTION, so these
# SQL commands are for testing the initial data copy using logical replication.
#
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rowfilter_1 (a, b) VALUES (1, 'not replicated')");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rowfilter_1 (a, b) VALUES (1500, 'filtered')");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rowfilter_1 (a, b) VALUES (1980, 'not filtered')");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rowfilter_1 (a, b) SELECT x, 'test ' || x FROM generate_series(990,1002) x"
);
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rowfilter_2 (c) SELECT generate_series(1, 20)");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rowfilter_3 (a, b) SELECT x, (x % 3 = 0) FROM generate_series(1, 10) x");

# insert data into partitioned table and directly on the partition
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rowfilter_partitioned (a, b) VALUES(1, 100),(7000, 101),(15000, 102),(5500, 300)");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rowfilter_less_10k (a, b) VALUES(2, 200),(6005, 201)");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rowfilter_greater_10k (a, b) VALUES(16000, 103)");

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
my $appname           = 'tap_sub';
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr application_name=$appname' PUBLICATION tap_pub_1, tap_pub_2, tap_pub_3"
);

$node_publisher->wait_for_catchup($appname);

# wait for initial table synchronization to finish
my $synced_query =
  "SELECT count(1) = 0 FROM pg_subscription_rel WHERE srsubstate NOT IN ('r', 's');";
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

# Check expected replicated rows for tab_rowfilter_1
# tap_pub_1 filter is: (a > 1000 AND b <> 'filtered')
# - INSERT (1, 'not replicated')   NO, because a is not > 1000
# - INSERT (1500, 'filtered')      NO, because b == 'filtered'
# - INSERT (1980, 'not filtered')  YES
# - generate_series(990,1002)      YES, only for 1001,1002 because a > 1000
#
my $result =
  $node_subscriber->safe_psql('postgres',
	"SELECT a, b FROM tab_rowfilter_1 ORDER BY 1, 2");
is( $result, qq(1001|test 1001
1002|test 1002
1980|not filtered), 'check initial data copy from table tab_rowfilter_1');

# Check expected replicated rows for tab_rowfilter_2
# tap_pub_1 filter is: (c % 2 = 0)
# tap_pub_2 filter is: (c % 3 = 0)
# When there are multiple publications for the same table, all filter
# expressions should succeed. In this case, rows are replicated if c value is
# divided by 2 AND 3 (6, 12, 18).
#
$result =
  $node_subscriber->safe_psql('postgres',
	"SELECT count(c), min(c), max(c) FROM tab_rowfilter_2");
is($result, qq(3|6|18), 'check initial data copy from table tab_rowfilter_2');

# Check expected replicated rows for tab_rowfilter_3
# There is no filter. 10 rows are inserted, so 10 rows are replicated.
$result =
  $node_subscriber->safe_psql('postgres',
	"SELECT count(a) FROM tab_rowfilter_3");
is($result, qq(10), 'check initial data copy from table tab_rowfilter_3');

# Check expected replicated rows for partitions
# publication option publish_via_partition_root is false so use the row filter
# from a partition
# tab_rowfilter_partitioned filter: (a < 5000)
# tab_rowfilter_less_10k filter:    (a < 6000)
# tab_rowfilter_greater_10k filter: no filter
#
# INSERT into tab_rowfilter_partitioned:
# - INSERT (1,100)       YES, because 1 < 6000
# - INSERT (7000, 101)   NO,  because 7000 is not < 6000
# - INSERT (15000, 102)  YES, because tab_rowfilter_greater_10k has no filter
# - INSERT (5500, 300)   YES, because 5500 < 6000
#
# INSERT directly into tab_rowfilter_less_10k:
# - INSERT (2, 200)      YES, because 2 < 6000
# - INSERT (6005, 201)   NO, because 6005 is not < 6000
#
# INSERT directly into tab_rowfilter_greater_10k:
# - INSERT (16000, 103)  YES, because tab_rowfilter_greater_10k has no filter
#
$result =
  $node_subscriber->safe_psql('postgres',
	"SELECT a, b FROM tab_rowfilter_less_10k ORDER BY 1, 2");
is($result, qq(1|100
2|200
5500|300), 'check initial data copy from partition tab_rowfilter_less_10k');

$result =
  $node_subscriber->safe_psql('postgres',
	"SELECT a, b FROM tab_rowfilter_greater_10k ORDER BY 1, 2");
is($result, qq(15000|102
16000|103), 'check initial data copy from partition tab_rowfilter_greater_10k');

# The following commands are executed after CREATE SUBSCRIPTION, so these SQL
# commands are for testing normal logical replication behavior.
#
# test row filter (INSERT, UPDATE, DELETE)
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rowfilter_1 (a, b) VALUES (800, 'test 800')");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rowfilter_1 (a, b) VALUES (1600, 'test 1600')");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rowfilter_1 (a, b) VALUES (1601, 'test 1601')");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rowfilter_1 (a, b) VALUES (1700, 'test 1700')");
$node_publisher->safe_psql('postgres',
	"UPDATE tab_rowfilter_1 SET b = NULL WHERE a = 1600");
$node_publisher->safe_psql('postgres',
	"UPDATE tab_rowfilter_1 SET b = 'test 1601 updated' WHERE a = 1601");
$node_publisher->safe_psql('postgres',
	"DELETE FROM tab_rowfilter_1 WHERE a = 1700");

$node_publisher->wait_for_catchup($appname);

# Check expected replicated rows for tab_rowfilter_1
# tap_pub_1 filter is: (a > 1000 AND b <> 'filtered')
#
# - 1001, 1002, 1980 already exist from initial data copy
# - INSERT (800, 'test 800')   NO, because 800 is not > 1000
# - INSERT (1600, 'test 1600') YES, because 1600 > 1000 and 'test 1600' <> 'filtered'
# - INSERT (1601, 'test 1601') YES, because 1601 > 1000 and 'test 1601' <> 'filtered'
# - INSERT (1700, 'test 1700') YES, because 1700 > 1000 and 'test 1700' <> 'filtered'
# - UPDATE (1600, NULL)        NO, row filter evaluates to false because NULL is not <> 'filtered'
# - UPDATE (1601, 'test 1601 updated') YES, because 1601 > 1000 and 'test 1601 updated' <> 'filtered'
# - DELETE (1700)              NO, row filter contains column b that is not part of
# the PK or REPLICA IDENTITY and old tuple contains b = NULL, hence, row filter
# evaluates to false
#
$result =
  $node_subscriber->safe_psql('postgres',
	"SELECT a, b FROM tab_rowfilter_1 ORDER BY 1, 2");
is($result, qq(1001|test 1001
1002|test 1002
1600|test 1600
1601|test 1601 updated
1700|test 1700
1980|not filtered), 'check replicated rows to table tab_rowfilter_1');

# Publish using root partitioned table
# Use a different partitioned table layout (exercise publish_via_partition_root)
$node_publisher->safe_psql('postgres',
	"ALTER PUBLICATION tap_pub_3 SET (publish_via_partition_root = true)");
$node_subscriber->safe_psql('postgres',
	"TRUNCATE TABLE tab_rowfilter_partitioned");
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub REFRESH PUBLICATION WITH (copy_data = true)");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rowfilter_partitioned (a, b) VALUES(4000, 400),(4001, 401),(4002, 402)");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rowfilter_less_10k (a, b) VALUES(4500, 450)");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rowfilter_less_10k (a, b) VALUES(5600, 123)");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rowfilter_greater_10k (a, b) VALUES(14000, 1950)");
$node_publisher->safe_psql('postgres',
	"UPDATE tab_rowfilter_less_10k SET b = 30 WHERE a = 4001");
$node_publisher->safe_psql('postgres',
	"DELETE FROM tab_rowfilter_less_10k WHERE a = 4002");

$node_publisher->wait_for_catchup($appname);

# Check expected replicated rows for partitions
# publication option publish_via_partition_root is true so use the row filter
# from the root partitioned table
# tab_rowfilter_partitioned filter: (a < 5000)
# tab_rowfilter_less_10k filter:    (a < 6000)
# tab_rowfilter_greater_10k filter: no filter
#
# After TRUNCATE, REFRESH PUBLICATION, the initial data copy will apply the
# partitioned table row filter.
# - INSERT (1, 100)      YES, 1 < 5000
# - INSERT (7000, 101)   NO, 7000 is not < 5000
# - INSERT (15000, 102)  NO, 15000 is not < 5000
# - INSERT (5500, 300)   NO, 5500 is not < 5000
# - INSERT (2, 200)      YES, 2 < 5000
# - INSERT (6005, 201)   NO, 6005 is not < 5000
# - INSERT (16000, 103)  NO, 16000 is not < 5000
#
# Execute SQL commands after initial data copy for testing the logical
# replication behavior.
# - INSERT (4000, 400)    YES, 4000 < 5000
# - INSERT (4001, 401)    YES, 4001 < 5000
# - INSERT (4002, 402)    YES, 4002 < 5000
# - INSERT (4500, 450)    YES, 4500 < 5000
# - INSERT (5600, 123)    NO, 5600 is not < 5000
# - INSERT (14000, 1950)  NO, 16000 is not < 5000
$result =
  $node_subscriber->safe_psql('postgres',
	"SELECT a, b FROM tab_rowfilter_partitioned ORDER BY 1, 2");
is( $result, qq(1|100
2|200
4000|400
4001|30
4500|450), 'check publish_via_partition_root behavior');

$node_subscriber->stop('fast');
$node_publisher->stop('fast');
