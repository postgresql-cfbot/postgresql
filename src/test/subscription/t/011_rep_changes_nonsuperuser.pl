# Basic logical replication test
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

if ($windows_os)
{
	plan skip_all => "authentication tests cannot run on Windows";
}
else
{
	plan tests => 18;
}

sub reset_pg_hba
{
	my $node       = shift;
	my $hba_method = shift;

	unlink($node->data_dir . '/pg_hba.conf');
	$node->append_conf('pg_hba.conf', "local all normal $hba_method");
	$node->append_conf('pg_hba.conf', "local all all trust");
	$node->reload;
	return;
}

# Initialize publisher node
my $node_publisher = get_new_node('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

# Create subscriber node
my $node_subscriber = get_new_node('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->start;

$node_subscriber->safe_psql('postgres',
	"SET password_encryption='md5'; CREATE ROLE normal LOGIN PASSWORD 'pass';");
$node_subscriber->safe_psql('postgres',
	"GRANT CREATE ON DATABASE postgres TO normal;");
$node_subscriber->safe_psql('postgres',
	"ALTER ROLE normal WITH LOGIN;");
reset_pg_hba($node_subscriber, 'trust');


$node_publisher->safe_psql('postgres',
	"SET password_encryption='md5'; CREATE ROLE normal LOGIN PASSWORD 'pass';");
$node_publisher->safe_psql('postgres',
	"ALTER ROLE normal WITH LOGIN; ALTER ROLE normal WITH SUPERUSER");
reset_pg_hba($node_publisher, 'md5');


# Create some preexisting content on publisher
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab_notrep AS SELECT generate_series(1,10) AS a");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab_ins AS SELECT generate_series(1,1002) AS a");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab_full AS SELECT generate_series(1,10) AS a");
$node_publisher->safe_psql('postgres', "CREATE TABLE tab_full2 (x text)");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_full2 VALUES ('a'), ('b'), ('b')");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab_rep (a int primary key)");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab_mixed (a int primary key, b text)");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_mixed (a, b) VALUES (1, 'foo')");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab_include (a int, b text, CONSTRAINT covering PRIMARY KEY(a) INCLUDE(b))"
);

# Setup structure on subscriber
$node_subscriber->safe_psql('postgres', "CREATE TABLE tab_notrep (a int)", extra_params => [ '-U', 'normal' ]);
$node_subscriber->safe_psql('postgres', "CREATE TABLE tab_ins (a int)", extra_params => [ '-U', 'normal' ]);
$node_subscriber->safe_psql('postgres', "CREATE TABLE tab_full (a int)", extra_params => [ '-U', 'normal' ]);
$node_subscriber->safe_psql('postgres', "CREATE TABLE tab_full2 (x text)", extra_params => [ '-U', 'normal' ]);
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab_rep (a int primary key)", extra_params => [ '-U', 'normal' ]);

# different column count and order than on publisher
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab_mixed (c text, b text, a int primary key)", extra_params => [ '-U', 'normal' ]);

# replication of the table with included index
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab_include (a int, b text, CONSTRAINT covering PRIMARY KEY(a) INCLUDE(b))"
, extra_params => [ '-U', 'normal' ]);

# Setup logical replication
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres', "CREATE PUBLICATION tap_pub");
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub_ins_only WITH (publish = insert)");
$node_publisher->safe_psql('postgres',
	"ALTER PUBLICATION tap_pub ADD TABLE tab_rep, tab_full, tab_full2, tab_mixed, tab_include, tab_notrep"
);
$node_publisher->safe_psql('postgres',
	"ALTER PUBLICATION tap_pub_ins_only ADD TABLE tab_ins");

my $appname = 'tap_sub';
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr password=pass user=normal application_name=$appname'
	PUBLICATION tap_pub, tap_pub_ins_only
	FOR TABLE tab_rep, tab_full, tab_full2, tab_mixed, tab_include, tab_ins",
 extra_params => [ '-U', 'normal' ]);

$node_publisher->wait_for_catchup($appname);

# Also wait for initial table sync to finish
my $synced_query =
  "SELECT count(1) = 0 FROM pg_subscription_rel WHERE srsubstate NOT IN ('r', 's');";
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

my $result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM tab_notrep");
is($result, qq(0), 'check non-replicated table is empty on subscriber');

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM tab_ins");
is($result, qq(1002), 'check initial data was copied to subscriber');

$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_ins SELECT generate_series(1,50)");
$node_publisher->safe_psql('postgres', "DELETE FROM tab_ins WHERE a > 20");
$node_publisher->safe_psql('postgres', "UPDATE tab_ins SET a = -a");

$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rep SELECT generate_series(1,50)");
$node_publisher->safe_psql('postgres', "DELETE FROM tab_rep WHERE a > 20");
$node_publisher->safe_psql('postgres', "UPDATE tab_rep SET a = -a");

$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_mixed VALUES (2, 'bar')");

$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_include SELECT generate_series(1,50)");
$node_publisher->safe_psql('postgres',
	"DELETE FROM tab_include WHERE a > 20");
$node_publisher->safe_psql('postgres', "UPDATE tab_include SET a = -a");

$node_publisher->wait_for_catchup($appname);

$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab_ins");
is($result, qq(1052|1|1002), 'check replicated inserts on subscriber');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab_rep");
is($result, qq(20|-20|-1), 'check replicated changes on subscriber');

$result =
  $node_subscriber->safe_psql('postgres', "SELECT c, b, a FROM tab_mixed");
is( $result, qq(|foo|1
|bar|2), 'check replicated changes with different column order');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab_include");
is($result, qq(20|-20|-1),
	'check replicated changes with primary key index with included columns');

# insert some duplicate rows
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_full SELECT generate_series(1,10)");

# add REPLICA IDENTITY FULL so we can update
$node_publisher->safe_psql('postgres',
	"ALTER TABLE tab_full REPLICA IDENTITY FULL");
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE tab_full REPLICA IDENTITY FULL");
$node_publisher->safe_psql('postgres',
	"ALTER TABLE tab_full2 REPLICA IDENTITY FULL");
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE tab_full2 REPLICA IDENTITY FULL");
$node_publisher->safe_psql('postgres',
	"ALTER TABLE tab_ins REPLICA IDENTITY FULL");
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE tab_ins REPLICA IDENTITY FULL");

# and do the updates
$node_publisher->safe_psql('postgres', "UPDATE tab_full SET a = a * a");
$node_publisher->safe_psql('postgres',
	"UPDATE tab_full2 SET x = 'bb' WHERE x = 'b'");

$node_publisher->wait_for_catchup($appname);

$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab_full");
is($result, qq(20|1|100),
	'update works with REPLICA IDENTITY FULL and duplicate tuples');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT x FROM tab_full2 ORDER BY 1");
is( $result, qq(a
bb
bb),
	'update works with REPLICA IDENTITY FULL and text datums');

# check that change of connection string and/or publication list causes
# restart of subscription workers. Not all of these are registered as tests
# as we need to poll for a change but the test suite will fail none the less
# when something goes wrong.
my $oldpid = $node_publisher->safe_psql('postgres',
	"SELECT pid FROM pg_stat_replication WHERE application_name = '$appname';"
);
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub CONNECTION 'application_name=$appname $publisher_connstr'"
);
$node_publisher->poll_query_until('postgres',
	"SELECT pid != $oldpid FROM pg_stat_replication WHERE application_name = '$appname';"
) or die "Timed out while waiting for apply to restart";

$oldpid = $node_publisher->safe_psql('postgres',
	"SELECT pid FROM pg_stat_replication WHERE application_name = '$appname';"
);
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub SET PUBLICATION tap_pub_ins_only WITH (copy_data = false)"
);
$node_publisher->poll_query_until('postgres',
	"SELECT pid != $oldpid FROM pg_stat_replication WHERE application_name = '$appname';"
) or die "Timed out while waiting for apply to restart";

$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_ins SELECT generate_series(1001,1100)");
$node_publisher->safe_psql('postgres', "DELETE FROM tab_rep");

# Restart the publisher and check the state of the subscriber which
# should be in a streaming state after catching up.
$node_publisher->stop('fast');
$node_publisher->start;

$node_publisher->wait_for_catchup($appname);

$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab_ins");
is($result, qq(1152|1|1100),
	'check replicated inserts after subscription publication change');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab_rep");
is($result, qq(20|-20|-1),
	'check changes skipped after subscription publication change');

# check alter publication (relcache invalidation etc)
$node_publisher->safe_psql('postgres',
	"ALTER PUBLICATION tap_pub_ins_only SET (publish = 'insert, delete')");
$node_publisher->safe_psql('postgres',
	"ALTER PUBLICATION tap_pub_ins_only ADD TABLE tab_full");
$node_publisher->safe_psql('postgres', "DELETE FROM tab_ins WHERE a > 0");

$result = $node_subscriber->safe_psql('postgres',
        "ALTER SUBSCRIPTION tap_sub ADD TABLE tab_full WITH (copy_data = false)");

$node_publisher->safe_psql('postgres', "INSERT INTO tab_full VALUES(0)");

$node_publisher->wait_for_catchup($appname);

# note that data are different on provider and subscriber
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab_ins");
is($result, qq(1052|1|1002),
	'check replicated deletes after alter publication');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab_full");
is($result, qq(21|0|100), 'check replicated insert after alter publication');

# check drop table from subscription
$result = $node_subscriber->safe_psql('postgres',
        "ALTER SUBSCRIPTION tap_sub DROP TABLE tab_full");

$node_publisher->safe_psql('postgres', "INSERT INTO tab_full VALUES(-1)");

$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(a), max(a) FROM tab_full");
is($result, qq(21|0|100), 'check replicated insert after alter publication');

# check restart on rename
$oldpid = $node_publisher->safe_psql('postgres',
	"SELECT pid FROM pg_stat_replication WHERE application_name = '$appname';"
);
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub RENAME TO tap_sub_renamed");
$node_publisher->poll_query_until('postgres',
	"SELECT pid != $oldpid FROM pg_stat_replication WHERE application_name = '$appname';"
) or die "Timed out while waiting for apply to restart";

# check all the cleanup
$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub_renamed");

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

$result = $node_publisher->safe_psql('postgres',
	"SELECT count(*) FROM pg_replication_slots");
is($result, qq(0), 'check replication slot was dropped on publisher');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM pg_replication_origin");
is($result, qq(0), 'check replication origin was dropped on subscriber');

$node_subscriber->stop('fast');
$node_publisher->stop('fast');
