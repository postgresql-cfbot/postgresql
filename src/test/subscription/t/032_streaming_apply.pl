# Copyright (c) 2022, PostgreSQL Global Development Group

# Test the restrictions of streaming mode "parallel" in logical replication

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $offset = 0;

# Create publisher node
my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf('postgresql.conf',
	'logical_decoding_work_mem = 64kB');
$node_publisher->start;

# Create subscriber node
my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init;
$node_subscriber->start;

# Setup structure on publisher
$node_publisher->safe_psql('postgres',
	"CREATE TABLE test_tab (a int primary key, b varchar)");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE test_tab_partitioned (a int primary key, b varchar)");

# Setup structure on subscriber
# We need to test normal table and partition table.
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE test_tab (a int primary key, b varchar)");
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE test_tab_partitioned (a int primary key, b varchar) PARTITION BY RANGE(a)");
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE test_tab_partition (LIKE test_tab_partitioned)");
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE test_tab_partitioned ATTACH PARTITION test_tab_partition DEFAULT"
);

# Setup logical replication
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub FOR TABLE test_tab");
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub_partitioned FOR TABLE test_tab_partitioned");

my $appname = 'tap_sub';
$node_subscriber->safe_psql(
	'postgres', "
	CREATE SUBSCRIPTION tap_sub
	CONNECTION '$publisher_connstr application_name=$appname'
	PUBLICATION tap_pub, tap_pub_partitioned
	WITH (streaming = parallel, copy_data = false)");

$node_publisher->wait_for_catchup($appname);

# ============================================================================
# It is not allowed that the unique index on the publisher and the subscriber
# is different. Check the error reported by background worker in this case. And
# after retrying in apply worker, we check if the data is replicated
# successfully.
# ============================================================================

# First we check the unique index on normal table.
$node_subscriber->safe_psql('postgres',
	"CREATE UNIQUE INDEX test_tab_b_idx ON test_tab (b)");

$node_publisher->safe_psql('postgres',
	"INSERT INTO test_tab SELECT i, md5(i::text) FROM generate_series(1, 5000) s(i)"
);

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? cannot replicate target relation "public.test_tab" using subscription parameter streaming=parallel/,
	$offset);

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

my $result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab");
is($result, qq(5000), 'data replicated to subscribers after retrying because of unique index');

# Drop the unique index on the subscriber.
$node_subscriber->safe_psql('postgres', "DROP INDEX test_tab_b_idx");

# Then we check the unique index on partition table.
$node_subscriber->safe_psql('postgres',
	"CREATE UNIQUE INDEX test_tab_b_partition_idx ON test_tab_partition (b)");

# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$node_publisher->safe_psql('postgres',
	"INSERT INTO test_tab_partitioned SELECT i, md5(i::text) FROM generate_series(1, 5000) s(i)"
);

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? cannot replicate target relation "public.test_tab_partitioned" using subscription parameter streaming=parallel/,
	$offset);

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab_partitioned");
is($result, qq(5000), 'data replicated to subscribers after retrying because of unique index');

# Drop the unique index on the subscriber.
$node_subscriber->safe_psql('postgres', "DROP INDEX test_tab_b_partition_idx");

# Triggers which execute non-immutable function are not allowed on the
# subscriber side. Check the error reported by background worker in this case.
# And after retrying in apply worker, we check if the data is replicated
# successfully.
# First we check the trigger function on normal table.
$node_subscriber->safe_psql(
	'postgres', qq{
CREATE FUNCTION trigger_func() RETURNS TRIGGER AS \$\$
  BEGIN
    RETURN NULL;
  END
\$\$ language plpgsql;
CREATE TRIGGER insert_trig
BEFORE INSERT ON test_tab
FOR EACH ROW EXECUTE PROCEDURE trigger_func();
ALTER TABLE test_tab ENABLE REPLICA TRIGGER insert_trig;
});

# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$node_publisher->safe_psql('postgres', "DELETE FROM test_tab");

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? cannot replicate target relation "public.test_tab" using subscription parameter streaming=parallel/,
	$offset);

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab");
is($result, qq(0), 'data replicated to subscribers after retrying because of trigger');

# Drop the trigger on the subscriber.
$node_subscriber->safe_psql('postgres',
	"DROP TRIGGER insert_trig ON test_tab");

# Then we check the trigger function on partition table.
$node_subscriber->safe_psql(
	'postgres', qq{
CREATE TRIGGER insert_trig
BEFORE INSERT ON test_tab_partition
FOR EACH ROW EXECUTE PROCEDURE trigger_func();
ALTER TABLE test_tab_partition ENABLE REPLICA TRIGGER insert_trig;
});

# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$node_publisher->safe_psql('postgres', "DELETE FROM test_tab_partitioned");

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? cannot replicate target relation "public.test_tab_partitioned" using subscription parameter streaming=parallel/,
	$offset);

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab_partitioned");
is($result, qq(0), 'data replicated to subscribers after retrying because of trigger');

# Drop the trigger on the subscriber.
$node_subscriber->safe_psql('postgres',
	"DROP TRIGGER insert_trig ON test_tab_partition");

# ============================================================================
# It is not allowed that column default value expression contains a
# non-immutable function on the subscriber side. Check the error reported by
# background worker in this case. And after retrying in apply worker, we check
# if the data is replicated successfully.
# ============================================================================

# First we check the column default value expression on normal table.
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE test_tab ALTER COLUMN b SET DEFAULT random()");

# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$node_publisher->safe_psql('postgres',
	"INSERT INTO test_tab SELECT i, md5(i::text) FROM generate_series(1, 5000) s(i)"
);

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? cannot replicate target relation "public.test_tab" using subscription parameter streaming=parallel/,
	$offset);

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab");
is($result, qq(5000),
	'data replicated to subscribers after retrying because of column default value');

# Drop default value on the subscriber.
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE test_tab ALTER COLUMN b DROP DEFAULT");

# Then we check the column default value expression on partition table.
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE test_tab_partition ALTER COLUMN b SET DEFAULT random()");

# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$node_publisher->safe_psql('postgres',
	"INSERT INTO test_tab_partitioned SELECT i, md5(i::text) FROM generate_series(1, 5000) s(i)"
);

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? cannot replicate target relation "public.test_tab_partitioned" using subscription parameter streaming=parallel/,
	$offset);

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab_partitioned");
is($result, qq(5000),
	'data replicated to subscribers after retrying because of column default value');

# Drop default value on the subscriber.
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE test_tab_partition ALTER COLUMN b DROP DEFAULT");

# ============================================================================
# It is not allowed that domain constraint expression contains a non-immutable
# function on the subscriber side. Check the error reported by background
# worker in this case. And after retrying in apply worker, we check if the data
# is replicated successfully.
# ============================================================================

# Because the column type of the partition table must be the same as its parent
# table, only test normal table here.
$node_subscriber->safe_psql(
	'postgres', qq{
CREATE DOMAIN test_domain AS int CHECK(VALUE > random());
ALTER TABLE test_tab ALTER COLUMN a TYPE test_domain;
});

# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$node_publisher->safe_psql('postgres', "DELETE FROM test_tab");

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? cannot replicate target relation "public.test_tab" using subscription parameter streaming=parallel/,
	$offset);

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab");
is($result, qq(0),
	'data replicated to subscribers after retrying because of domain'
);

# Drop domain constraint expression on the subscriber.
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE test_tab ALTER COLUMN a TYPE int");

# ============================================================================
# It is not allowed that constraint expression contains a non-immutable
# function on the subscriber side. Check the error reported by background
# worker in this case. And after retrying in apply worker, we check if the data
# is replicated successfully.
# ============================================================================

# First we check the constraint expression on normal table.
$node_subscriber->safe_psql(
	'postgres', qq{
ALTER TABLE test_tab ADD CONSTRAINT test_tab_con check (a > random());
});

# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$node_publisher->safe_psql('postgres',
	"INSERT INTO test_tab SELECT i, md5(i::text) FROM generate_series(1, 5000) s(i)"
);

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? cannot replicate target relation "public.test_tab" using subscription parameter streaming=parallel/,
	$offset);

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab");
is($result, qq(5000),
	'data replicated to subscribers after retrying because of constraint');

# Drop constraint on the subscriber.
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE test_tab DROP CONSTRAINT test_tab_con");

# Then we check the constraint expression on partition table.
$node_subscriber->safe_psql(
	'postgres', qq{
ALTER TABLE test_tab_partition ADD CONSTRAINT test_tab_con check (a > random());
});

# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$node_publisher->safe_psql('postgres', "DELETE FROM test_tab_partitioned");

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? cannot replicate target relation "public.test_tab_partitioned" using subscription parameter streaming=parallel/,
	$offset);

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab_partitioned");
is($result, qq(0),
	'data replicated to subscribers after retrying because of constraint');

# Drop constraint on the subscriber.
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE test_tab_partition DROP CONSTRAINT test_tab_con");

# ============================================================================
# It is not allowed that foreign key on the subscriber side. Check the error
# reported by background worker in this case. And after retrying in apply
# worker, we check if the data is replicated successfully.
# ============================================================================

# First we check the foreign key on normal table.
$node_publisher->safe_psql('postgres', "DELETE FROM test_tab");
$node_publisher->wait_for_catchup($appname);
$node_subscriber->safe_psql(
	'postgres', qq{
CREATE TABLE test_tab_f (a int primary key);
ALTER TABLE test_tab ADD CONSTRAINT test_tabfk FOREIGN KEY(a) REFERENCES test_tab_f(a);
});

# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$node_publisher->safe_psql('postgres',
	"INSERT INTO test_tab SELECT i, md5(i::text) FROM generate_series(1, 5000) s(i)"
);

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? cannot replicate target relation "public.test_tab" using subscription parameter streaming=parallel/,
	$offset);

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab");
is($result, qq(5000),
	'data replicated to subscribers after retrying because of foreign key');

# Drop the foreign key constraint on the subscriber.
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE test_tab DROP CONSTRAINT test_tabfk");

# Then we check the foreign key on partition table.
$node_publisher->wait_for_catchup($appname);
$node_subscriber->safe_psql(
	'postgres', qq{
CREATE TABLE test_tab_partition_f (a int primary key);
ALTER TABLE test_tab_partition ADD CONSTRAINT test_tab_patition_fk FOREIGN KEY(a) REFERENCES test_tab_partition_f(a);
});

# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$node_publisher->safe_psql('postgres',
	"INSERT INTO test_tab_partitioned SELECT i, md5(i::text) FROM generate_series(1, 5000) s(i)"
);

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? cannot replicate target relation "public.test_tab_partitioned" using subscription parameter streaming=parallel/,
	$offset);

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab_partitioned");
is($result, qq(5000),
	'data replicated to subscribers after retrying because of foreign key');

# Drop the foreign key constraint on the subscriber.
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE test_tab_partition DROP CONSTRAINT test_tab_patition_fk");

$node_subscriber->stop;
$node_publisher->stop;

done_testing();
