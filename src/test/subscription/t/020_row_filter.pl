# Test logical replication behavior with row filtering
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 6;

# create publisher node
my $node_publisher = get_new_node('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf('postgresql.conf', 'log_min_messages = DEBUG2');
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

# test row filtering
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
	"INSERT INTO tab_rowfilter_3 (a, b) SELECT x, (x % 3 = 0) FROM generate_series(1, 10) x"
);
# use partition row filter:
# - replicate (1, 100) because 1 < 6000 is true
# - don't replicate (8000, 101) because 8000 < 6000 is false
# - replicate (15000, 102) because partition tab_rowfilter_greater_10k doesn't have row filter
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rowfilter_partitioned (a, b) VALUES(1, 100),(8000, 101),(15000, 102)"
);
# insert directly into partition
# use partition row filter: replicate (2, 200) because 2 < 6000 is true
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rowfilter_less_10k (a, b) VALUES(2, 200)");
# use partition row filter: replicate (5500, 300) because 5500 < 6000 is true
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rowfilter_partitioned (a, b) VALUES(5500, 300)");

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
my $appname           = 'tap_sub';
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr application_name=$appname' PUBLICATION tap_pub_1, tap_pub_2, tap_pub_3"
);

$node_publisher->wait_for_catchup($appname);

# wait for initial table sync to finish
my $synced_query =
  "SELECT count(1) = 0 FROM pg_subscription_rel WHERE srsubstate NOT IN ('r', 's');";
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

my $result =
  $node_subscriber->safe_psql('postgres',
	"SELECT a, b FROM tab_rowfilter_1 ORDER BY 1, 2");
is( $result, qq(1001|test 1001
1002|test 1002
1980|not filtered), 'check filtered data was copied to subscriber');

$result =
  $node_subscriber->safe_psql('postgres',
	"SELECT count(c), min(c), max(c) FROM tab_rowfilter_2");
is($result, qq(3|6|18), 'check filtered data was copied to subscriber');

$result =
  $node_subscriber->safe_psql('postgres',
	"SELECT count(a) FROM tab_rowfilter_3");
is($result, qq(10), 'check filtered data was copied to subscriber');

$result =
  $node_subscriber->safe_psql('postgres',
	"SELECT a, b FROM tab_rowfilter_less_10k ORDER BY 1, 2");
is( $result, qq(1|100
2|200
5500|300), 'check filtered data was copied to subscriber');

$result =
  $node_subscriber->safe_psql('postgres',
	"SELECT a, b FROM tab_rowfilter_greater_10k ORDER BY 1, 2");
is($result, qq(15000|102), 'check filtered data was copied to subscriber');

# publish using partitioned table
$node_publisher->safe_psql('postgres',
	"ALTER PUBLICATION tap_pub_3 SET (publish_via_partition_root = true)");
$node_subscriber->safe_psql('postgres',
	"TRUNCATE TABLE tab_rowfilter_partitioned");
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub REFRESH PUBLICATION WITH (copy_data = true)");
# use partitioned table row filter: replicate, 4000 < 5000 is true
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rowfilter_partitioned (a, b) VALUES(4000, 400)");
# use partitioned table row filter: replicate, 4500 < 5000 is true
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rowfilter_less_10k (a, b) VALUES(4500, 450)");
# use partitioned table row filter: don't replicate, 5600 < 5000 is false
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rowfilter_less_10k (a, b) VALUES(5600, 123)");
# use partitioned table row filter: don't replicate, 16000 < 5000 is false
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rowfilter_greater_10k (a, b) VALUES(16000, 1950)");

$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres',
	"SELECT a, b FROM tab_rowfilter_partitioned ORDER BY 1, 2");
is( $result, qq(1|100
2|200
4000|400
4500|450), 'check publish_via_partition_root behavior');

$node_subscriber->stop('fast');
$node_publisher->stop('fast');
