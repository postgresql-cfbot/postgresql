# Copyright (c) 2022, PostgreSQL Global Development Group

# Test the safety checks of streaming mode "parallel" in logical replication.
# Without these safety checks, the subscriber's apply worker may fall into an
# infinite wait without the user knowing.
#
# For normal tables, we use deadlock-producing test cases to ensure that future
# modifications do not invalidate constraint checks.
#
# For partitioned tables, we just use test cases to confirm that the constraint
# checks are as expected.

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
$node_publisher->safe_psql('postgres', "CREATE TABLE test_tab1 (a int)");
$node_publisher->safe_psql('postgres', "CREATE TABLE test_tab2 (a int)");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE test_tab_partitioned (a int primary key, b varchar)");

# Setup structure on subscriber
# We need to test normal table and partition table.
$node_subscriber->safe_psql('postgres', "CREATE TABLE test_tab1 (a int)");
$node_subscriber->safe_psql('postgres', "CREATE TABLE test_tab2 (a int)");
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE test_tab_partitioned (a int primary key, b varchar) PARTITION BY RANGE(a)"
);
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE test_tab_partition (LIKE test_tab_partitioned)");
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE test_tab_partitioned ATTACH PARTITION test_tab_partition DEFAULT"
);

# Setup logical replication
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub_normal FOR TABLE test_tab1, test_tab2");
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub_partitioned FOR TABLE test_tab_partitioned");

my $appname = 'tap_sub';
$node_subscriber->safe_psql(
	'postgres', "
	CREATE SUBSCRIPTION tap_sub
	CONNECTION '$publisher_connstr application_name=$appname'
	PUBLICATION tap_pub_normal, tap_pub_partitioned
	WITH (streaming = parallel, copy_data = false)");

$node_publisher->wait_for_catchup($appname);

# Interleave a pair of transactions, each exceeding the 64kB limit.
my $in  = '';
my $out = '';

my $timer = IPC::Run::timeout($PostgreSQL::Test::Utils::timeout_default);

my $h = $node_publisher->background_psql('postgres', \$in, \$out, $timer,
	on_error_stop => 0);

# ============================================================================
# It is not allowed that the unique column in the relation on the
# subscriber-side is not the unique column on the publisher-side. Check the
# error reported by parallel worker in this case. And after retrying in
# apply worker, we check if the data is replicated successfully.
# ============================================================================

# First we check the unique index on normal table.
$node_subscriber->safe_psql('postgres',
	"CREATE UNIQUE INDEX idx_tab1 on test_tab1(a)");

$in .= q{
BEGIN;
INSERT INTO test_tab1 SELECT i FROM generate_series(1, 5000) s(i);
};
$h->pump_nb;

$node_publisher->safe_psql('postgres', "INSERT INTO test_tab1 values(1)");

$in .= q{
COMMIT;
\q
};
$h->finish;

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? cannot replicate target relation "public.test_tab1" using subscription parameter streaming = parallel/,
	$offset);

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? duplicate key value violates unique constraint "idx_tab1"/,
	$offset);

# Drop the unique index on the subscriber, now it works.
$node_subscriber->safe_psql('postgres', "DROP INDEX idx_tab1");

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

my $result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab1");
is($result, qq(5001),
	'data replicated to subscriber after dropping unique index to retry apply'
);

# Clean up test data from the environment.
$node_publisher->safe_psql('postgres', "TRUNCATE TABLE test_tab1");
$node_publisher->wait_for_catchup($appname);

# Then we check the unique index on partition table.
$node_subscriber->safe_psql('postgres',
	"CREATE UNIQUE INDEX test_tab_b_partition_idx ON test_tab_partition (b)");

# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$node_publisher->safe_psql('postgres',
	"INSERT INTO test_tab_partitioned SELECT i, md5(i::text) FROM generate_series(1, 5000) s(i)"
);

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? cannot replicate target relation "public.test_tab_partitioned" using subscription parameter streaming = parallel/,
	$offset);

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM test_tab_partitioned");
is($result, qq(5000),
	'data replicated to subscriber after retrying because of unique index');

# Drop the unique index on the subscriber.
$node_subscriber->safe_psql('postgres',
	"DROP INDEX test_tab_b_partition_idx");

# ============================================================================
# Triggers which execute non-immutable function are not allowed on the
# subscriber side. Check the error reported by parallel worker in this case.
# And after retrying in apply worker, we check if the data is replicated
# successfully.
# ============================================================================

# First we check the trigger function on normal table.
$node_publisher->safe_psql('postgres',
	"CREATE UNIQUE INDEX idx_tab2 on test_tab2(a)");
$node_subscriber->safe_psql('postgres',
	"CREATE UNIQUE INDEX idx_tab2 on test_tab2(a)");

$node_subscriber->safe_psql(
	'postgres', qq{
CREATE FUNCTION trigger_func_tab1_unsafe() RETURNS TRIGGER AS \$\$
  BEGIN
    INSERT INTO public.test_tab2 VALUES (NEW.*);
    RETURN NEW;
  END
\$\$ language plpgsql;
CREATE TRIGGER tri_tab1_unsafe
BEFORE INSERT ON public.test_tab1
FOR EACH ROW EXECUTE PROCEDURE trigger_func_tab1_unsafe();
ALTER TABLE test_tab1 ENABLE REPLICA TRIGGER tri_tab1_unsafe;
});

# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$in .= q{
BEGIN;
INSERT INTO test_tab1 VALUES(5001);
INSERT INTO test_tab2 SELECT i FROM generate_series(1, 5000) s(i);
};
$h->pump_nb;

$node_publisher->safe_psql('postgres', "INSERT INTO test_tab1 VALUES(5001)");

$in .= q{
COMMIT;
\q
};
$h->finish;

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? cannot replicate target relation "public.test_tab1" using subscription parameter streaming = parallel/,
	$offset);

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? duplicate key value violates unique constraint "idx_tab2"/,
	$offset);

# Drop the unique index on the subscriber, now it works.
$node_subscriber->safe_psql('postgres', "DROP INDEX idx_tab2");

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab1");
is($result, qq(2),
	'data replicated to subscriber after retrying because of trigger');

# Clean up test data from the environment.
$node_subscriber->safe_psql(
	'postgres', qq{
DROP TRIGGER tri_tab1_unsafe ON public.test_tab1;
DROP function trigger_func_tab1_unsafe;
});
$node_publisher->safe_psql(
	'postgres', qq{
DROP INDEX idx_tab2;
TRUNCATE TABLE test_tab1;
TRUNCATE TABLE test_tab2;
});
$node_publisher->wait_for_catchup($appname);

# Then we check the trigger function on partition table.
$node_subscriber->safe_psql(
	'postgres', qq{
CREATE FUNCTION trigger_func() RETURNS TRIGGER AS \$\$
  BEGIN
    RETURN NULL;
  END
\$\$ language plpgsql;
CREATE TRIGGER insert_trig
BEFORE INSERT ON test_tab_partition
FOR EACH ROW EXECUTE PROCEDURE trigger_func();
ALTER TABLE test_tab_partition ENABLE REPLICA TRIGGER insert_trig;
});

# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$node_publisher->safe_psql('postgres', "DELETE FROM test_tab_partitioned");

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? cannot replicate target relation "public.test_tab_partitioned" using subscription parameter streaming = parallel/,
	$offset);

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM test_tab_partitioned");
is($result, qq(0),
	'data replicated to subscriber after retrying because of trigger');

# Drop the trigger on the subscriber.
$node_subscriber->safe_psql('postgres',
	"DROP TRIGGER insert_trig ON test_tab_partition");

# ============================================================================
# It is not allowed that column default value expression contains a
# non-immutable function on the subscriber side. Check the error reported by
# parallel worker in this case. And after retrying in apply worker, we check
# if the data is replicated successfully.
# ============================================================================

# First we check the column default value expression on normal table.
$node_publisher->safe_psql('postgres', "INSERT INTO test_tab2 VALUES(1)");

$node_subscriber->safe_psql(
	'postgres', qq{
CREATE FUNCTION func_count_tab2() RETURNS INT AS \$\$
  BEGIN
    RETURN (SELECT count(*) FROM public.test_tab2);
  END
\$\$ language plpgsql;
ALTER TABLE test_tab1 ADD COLUMN b int DEFAULT func_count_tab2();
});

# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$in .= q{
BEGIN;
TRUNCATE test_tab2;
INSERT INTO test_tab1 SELECT i FROM generate_series(1, 5000) s(i);
};
$h->pump_nb;

$node_publisher->safe_psql('postgres', "INSERT INTO test_tab1(a) VALUES(1)");

$in .= q{
COMMIT;
\q
};
$h->finish;

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? cannot replicate target relation "public.test_tab1" using subscription parameter streaming = parallel/,
	$offset);

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab1");
is($result, qq(5001),
	'data replicated to subscriber after retrying because of column default value'
);

# Clean up test data from the environment.
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE test_tab1 DROP COLUMN b");
$node_publisher->safe_psql(
	'postgres', qq{
TRUNCATE TABLE test_tab1;
TRUNCATE TABLE test_tab2;
});
$node_publisher->wait_for_catchup($appname);

# Then we check the column default value expression on partition table.
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE test_tab_partition ALTER COLUMN b SET DEFAULT random()");

# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$node_publisher->safe_psql('postgres',
	"INSERT INTO test_tab_partitioned SELECT i, md5(i::text) FROM generate_series(1, 5000) s(i)"
);

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? cannot replicate target relation "public.test_tab_partitioned" using subscription parameter streaming = parallel/,
	$offset);

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM test_tab_partitioned");
is($result, qq(5000),
	'data replicated to subscriber after retrying because of column default value'
);

# Drop default value on the subscriber.
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE test_tab_partition ALTER COLUMN b DROP DEFAULT");

# ============================================================================
# It is not allowed that domain constraint expression contains a non-immutable
# function on the subscriber side. Check the error reported by parallel
# worker in this case. And after retrying in apply worker, we check if the
# data is replicated successfully.
# ============================================================================

# Because the column type of the partition table must be the same as its parent
# table, only test normal table here.
$node_publisher->safe_psql('postgres', "INSERT INTO test_tab2 VALUES(1)");

$node_publisher->safe_psql(
	'postgres', qq{
CREATE DOMAIN tmp_domain AS int CHECK (VALUE > -1);
ALTER TABLE test_tab1 ALTER COLUMN a TYPE tmp_domain;
});

$node_subscriber->safe_psql(
	'postgres', qq{
CREATE DOMAIN tmp_domain AS INT CONSTRAINT domain_check CHECK (VALUE >= func_count_tab2());
ALTER TABLE test_tab1 ALTER COLUMN a TYPE tmp_domain;
});

# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$in .= q{
BEGIN;
TRUNCATE test_tab2;
INSERT INTO test_tab1 SELECT i FROM generate_series(1, 5000) s(i);
};
$h->pump_nb;

$node_publisher->safe_psql('postgres', "INSERT INTO test_tab1(a) VALUES(1)");

$in .= q{
COMMIT;
\q
};
$h->finish;

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? cannot replicate target relation "public.test_tab1" using subscription parameter streaming = parallel/,
	$offset);

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab1");
is($result, qq(5001),
	'data replicated to subscriber after retrying because of domain');

# Clean up test data from the environment.
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE test_tab1 ALTER COLUMN a TYPE int");
$node_publisher->safe_psql(
	'postgres', qq{
TRUNCATE TABLE test_tab1;
TRUNCATE TABLE test_tab2;
});
$node_publisher->wait_for_catchup($appname);

# ============================================================================
# It is not allowed that constraint expression contains a non-immutable
# function on the subscriber side. Check the error reported by parallel
# worker in this case. And after retrying in apply worker, we check if the
# data is replicated successfully.
# ============================================================================

# First we check the constraint expression on normal table.
$node_publisher->safe_psql('postgres', "INSERT INTO test_tab2 VALUES(1)");

$node_subscriber->safe_psql(
	'postgres', qq{
ALTER TABLE test_tab1 ADD CONSTRAINT const_tab1_unsafe CHECK(a >= func_count_tab2());
});

# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$in .= q{
BEGIN;
TRUNCATE test_tab2;
INSERT INTO test_tab1 SELECT i FROM generate_series(1, 5000) s(i);
};
$h->pump_nb;

$node_publisher->safe_psql('postgres', "INSERT INTO test_tab1(a) VALUES(1)");

$in .= q{
COMMIT;
\q
};
$h->finish;

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? cannot replicate target relation "public.test_tab1" using subscription parameter streaming = parallel/,
	$offset);

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab1");
is($result, qq(5001),
	'data replicated to subscriber after retrying because of constraint');

# Clean up test data from the environment.
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE test_tab1 DROP CONSTRAINT const_tab1_unsafe");
$node_publisher->safe_psql(
	'postgres', qq{
TRUNCATE TABLE test_tab1;
TRUNCATE TABLE test_tab2;
});
$node_publisher->wait_for_catchup($appname);

# Then we check the constraint expression on partition table.
$node_subscriber->safe_psql(
	'postgres', qq{
ALTER TABLE test_tab_partition ADD CONSTRAINT test_tab_con check (a > random());
});

# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$node_publisher->safe_psql('postgres', "DELETE FROM test_tab_partitioned");

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? cannot replicate target relation "public.test_tab_partitioned" using subscription parameter streaming = parallel/,
	$offset);

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM test_tab_partitioned");
is($result, qq(0),
	'data replicated to subscriber after retrying because of constraint');

# Drop constraint on the subscriber.
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE test_tab_partition DROP CONSTRAINT test_tab_con");

# ============================================================================
# It is not allowed that foreign key on the subscriber side. Check the error
# reported by parallel worker in this case. And after retrying in apply
# worker, we check if the data is replicated successfully.
# ============================================================================

# First we check the foreign key on normal table.
$node_publisher->safe_psql(
	'postgres', qq{
CREATE TABLE tab_nopublic(a int);
ALTER TABLE test_tab2 ADD PRIMARY KEY (a);
ALTER TABLE test_tab2 REPLICA IDENTITY FULL;
INSERT INTO test_tab2 VALUES(1);
});
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE test_tab2 ADD PRIMARY KEY (a);");

$node_subscriber->safe_psql(
	'postgres', qq{
ALTER TABLE test_tab1 ADD CONSTRAINT test_tab1fk FOREIGN KEY(a) REFERENCES test_tab2(a);
SELECT 'ALTER TABLE test_tab1 ENABLE REPLICA TRIGGER "' || tgname || '"' FROM pg_trigger WHERE tgrelid = 'test_tab1'::regclass::oid \\gexec
});

# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$in .= q{
BEGIN;
INSERT INTO test_tab1(a) VALUES(1);
INSERT INTO tab_nopublic SELECT i FROM generate_series(1, 5000) s(i);
};
$h->pump_nb;

$node_publisher->safe_psql('postgres', "DELETE FROM test_tab2");

$in .= q{
COMMIT;
\q
};
$h->finish;

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? cannot replicate target relation "public.test_tab1" using subscription parameter streaming = parallel/,
	$offset);

# Wait for error log to make sure the dependent data has been deleted.
$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? insert or update on table "test_tab1" violates foreign key constraint "test_tab1fk"/,
	$offset);

# Insert dependent data on the publisher, now it works.
$node_subscriber->safe_psql('postgres', "INSERT INTO test_tab2 VALUES(1)");

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tab1");
is($result, qq(1),
	'data replicated to subscriber after retrying because of foreign key');

# Clean up test data from the environment.
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE test_tab1 DROP CONSTRAINT test_tab1fk");
$node_publisher->safe_psql(
	'postgres', qq{
TRUNCATE TABLE test_tab1;
TRUNCATE TABLE test_tab2;
});
$node_publisher->wait_for_catchup($appname);

# Then we check the foreign key on partition table.
$node_subscriber->safe_psql(
	'postgres', qq{
CREATE TABLE test_tab_partition_f (a int primary key);
INSERT INTO test_tab_partition_f SELECT i  FROM generate_series(1, 5000) s(i);
ALTER TABLE test_tab_partition ADD CONSTRAINT test_tab_patition_fk FOREIGN KEY(a) REFERENCES test_tab_partition_f(a);
SELECT 'ALTER TABLE test_tab_partition ENABLE REPLICA TRIGGER "' || tgname || '"' FROM pg_trigger WHERE tgrelid = 'test_tab_partition'::regclass::oid \\gexec
});

# Check the subscriber log from now on.
$offset = -s $node_subscriber->logfile;

$node_publisher->safe_psql('postgres',
	"INSERT INTO test_tab_partitioned SELECT i, md5(i::text) FROM generate_series(1, 5000) s(i)"
);

$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? cannot replicate target relation "public.test_tab_partitioned" using subscription parameter streaming = parallel/,
	$offset);

# Wait for this streaming transaction to be applied in the apply worker.
$node_publisher->wait_for_catchup($appname);

$result =
  $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM test_tab_partitioned");
is($result, qq(5000),
	'data replicated to subscribers after retrying because of foreign key');

# Drop the foreign key constraint on the subscriber.
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE test_tab_partition DROP CONSTRAINT test_tab_patition_fk");

$node_subscriber->stop;
$node_publisher->stop;

done_testing();
