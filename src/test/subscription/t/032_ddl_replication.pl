# Copyright (c) 2022, PostgreSQL Global Development Group
# Regression tests for logical replication of DDLs
#
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->start;

my $ddl = "CREATE TABLE test_rep(id int primary key, name varchar, value integer);";
$node_publisher->safe_psql('postgres', $ddl);
$node_publisher->safe_psql('postgres', "INSERT INTO test_rep VALUES (1, 'data1', 1);");
$node_subscriber->safe_psql('postgres', $ddl);

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION mypub FOR ALL TABLES with (publish = 'insert, update, delete, ddl');");
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION mysub CONNECTION '$publisher_connstr' PUBLICATION mypub;");
$node_publisher->wait_for_catchup('mysub');

# Make sure we have fully synchronized the table.
# This prevents ALTER TABLE command below from being executed during table synchronization.
$node_subscriber->poll_query_until('postgres',
   "SELECT COUNT(1) = 0 FROM pg_subscription_rel sr WHERE sr.srsubstate NOT IN ('s', 'r') AND sr.srrelid = 'test_rep'::regclass"
);

# Test ALTER TABLE ADD
$node_publisher->safe_psql('postgres', "ALTER TABLE test_rep ADD c4 int;");
$node_publisher->safe_psql('postgres', "INSERT INTO test_rep VALUES (2, 'data2', 2, 2);");
$node_publisher->wait_for_catchup('mysub');
my $result = $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_rep WHERE c4 = 2;");
is($result, qq(1), 'ALTER test_rep ADD replicated');

# Test ALTER TABLE DROP
$node_publisher->safe_psql('postgres', "ALTER TABLE test_rep DROP c4;");
$node_publisher->safe_psql('postgres', "DELETE FROM test_rep where id = 2;");
$node_publisher->wait_for_catchup('mysub');
$result = $node_subscriber->safe_psql('postgres', "SELECT count(*) from test_rep;");
is($result, qq(1), 'ALTER test_rep DROP replicated');

# Test ALTER TABLE ALTER TYPE
$node_publisher->safe_psql('postgres', "ALTER TABLE test_rep ALTER value TYPE varchar");
$node_publisher->safe_psql('postgres', "INSERT INTO test_rep VALUES (3, 'data3', '3');");
$node_publisher->wait_for_catchup('mysub');
$result = $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_rep WHERE value = '3';");
is($result, qq(1), 'ALTER test_rep ALTER COLUMN TYPE replicated');

# Test ALTER TABLE ALTER SET DEFAULT
# Check if we have the default value after the direct insert to subscriber node.
$node_publisher->safe_psql('postgres', "ALTER TABLE test_rep ALTER COLUMN value SET DEFAULT 'foo'");
$node_publisher->wait_for_catchup('mysub');
$node_subscriber->safe_psql('postgres', "INSERT INTO test_rep VALUES (4, 'data4');");
$result = $node_subscriber->safe_psql('postgres', "SELECT value FROM test_rep WHERE id = 4;");
is($result, 'foo', 'ALTER test_rep ALTER SET DEFAULT replicated');

# Test ALTER TABLE ALTER DROP DEFAULT
# Check if we don't have the default value previously defined.
$node_publisher->safe_psql('postgres', "ALTER TABLE test_rep ALTER COLUMN value DROP DEFAULT;");
$node_publisher->wait_for_catchup('mysub');
$node_subscriber->safe_psql('postgres', "INSERT INTO test_rep VALUES (5, 'data5');");
$result = $node_subscriber->safe_psql('postgres', "SELECT value IS NULL FROM test_rep WHERE id = 5;");
is($result, q(t), 'ALTER test_rep ALTER DROP DEFAULT replicated');

# Test ALTER TABLE ALTER SET NOT NULL
# Remove the existing record that contains null value first.
my ($stdout, $stderr);
$node_subscriber->safe_psql('postgres', "DELETE FROM test_rep WHERE id = 5;");
$node_publisher->safe_psql('postgres', "ALTER TABLE test_rep ALTER value SET NOT NULL;");
$node_publisher->wait_for_catchup('mysub');
$node_subscriber->psql('postgres', "INSERT INTO test_rep VALUES (6, 'data6');",
					   on_error_stop => 0,
					   stderr => \$stderr,
					   stdout => \$stdout);
$stderr =~ /ERROR:  null value in column \"value\" of relation \"test_rep\" violates not-null constraint/
  or die "failed to replicate ALTER TABLE ALTER SET NOT NULL";

# Test ALTER TABLE ALTER DROP NOT NULL
$node_publisher->safe_psql('postgres', "ALTER TABLE test_rep ALTER value DROP NOT NULL;");
$node_publisher->wait_for_catchup('mysub');
# Insert same data that has NULL value. This failed before but now should succeed.
$node_subscriber->safe_psql('postgres', "INSERT INTO test_rep VALUES (6, 'data6');");
$result = $node_subscriber->safe_psql('postgres', "SELECT value IS NULL FROM test_rep WHERE id = 6;");
is($result, q(t), "ALTER test_rep ALTER DROP NOT NULL replicated");

# Test ALTER TABLE SET UNLOGGED
$node_publisher->safe_psql('postgres', 'ALTER TABLE test_rep SET UNLOGGED;');
$node_publisher->wait_for_catchup('mysub');
$node_publisher->safe_psql('postgres', "INSERT INTO test_rep VALUES (7, 'data7', '7');");
$node_publisher->wait_for_catchup('mysub');
$result = $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_rep WHERE id = 7;");
is($result, qq(0), 'ALTER test_rep SET UNLOGGED replicated');

# Test ALTER TABLE SET LOGGED
$node_publisher->safe_psql('postgres', 'ALTER TABLE test_rep SET LOGGED;');
$node_publisher->wait_for_catchup('mysub');
$node_publisher->safe_psql('postgres', "INSERT INTO test_rep VALUES (8, 'data8', '8');");
$node_publisher->wait_for_catchup('mysub');
$result = $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM test_rep WHERE id = 8;");
is($result, qq(1), 'ALTER test_rep SET LOGGED replicated');

# Test CREATE TABLE and DML changes
$node_publisher->safe_psql('postgres', "CREATE TABLE tmp (a int, b varchar);");
$node_publisher->wait_for_catchup('mysub');
$result = $node_subscriber->safe_psql('postgres', "SELECT count(*) from pg_tables where tablename = 'tmp';");
is($result, qq(1), 'CREATE tmp is replicated');
$node_publisher->safe_psql('postgres', "INSERT INTO tmp values (1, 'a')");
$node_publisher->safe_psql('postgres', "INSERT INTO tmp values (2, 'b')");
$node_publisher->wait_for_catchup('mysub');
$result = $node_subscriber->safe_psql('postgres', "SELECT count(*) from tmp;");
is($result, qq(2), 'DML Changes to tmp are replicated');

# Test CREATE TABLE INHERITS
$node_publisher->safe_psql('postgres', "CREATE TABLE tmp2 (c3 int) INHERITS (tmp);");
$node_publisher->safe_psql('postgres', "INSERT INTO tmp2 VALUES (1, 'a', 1);");
$node_publisher->wait_for_catchup('mysub');
$result = $node_subscriber->safe_psql('postgres', "SELECT count(*) from pg_tables where tablename = 'tmp2';");
is($result, qq(1), 'CREATE TABLE INHERITS is replicated');
$result = $node_subscriber->safe_psql('postgres', "SELECT count(*) from tmp2;");
is($result, qq(1), 'inserting some data to inherited table replicated');
$node_publisher->safe_psql('postgres', "DROP TABLE tmp2");

# Test CREATE TABLE LIKE
$node_publisher->safe_psql('postgres', "CREATE TABLE tmp3 (c3 int, LIKE tmp);");
$node_publisher->safe_psql('postgres', "INSERT INTO tmp3 VALUES (1, 1, 'a');");
$node_publisher->wait_for_catchup('mysub');
$result = $node_subscriber->safe_psql('postgres', "SELECT count(*) from pg_tables where tablename = 'tmp3';");
is($result, qq(1), 'CREATE TABLE LIKE replicated');
$result = $node_subscriber->safe_psql('postgres', "SELECT count(*) from tmp3;");
is($result, qq(1), 'insert some data to a table defined with LIKE replicated');
$node_publisher->safe_psql('postgres', "DROP TABLE tmp3");

# Test DROP TABLE
$node_publisher->safe_psql('postgres', "DROP TABLE tmp;");
$node_publisher->wait_for_catchup('mysub');
$result = $node_subscriber->safe_psql('postgres', "SELECT count(*) from pg_tables where tablename = 'tmp';");
is($result, qq(0), 'TABLE tmp is dropped');

# Test CREATE UNLOGGED TABLE
$node_publisher->safe_psql('postgres', "CREATE UNLOGGED TABLE tmp (id int);");
$node_publisher->safe_psql('postgres', "INSERT INTO tmp VALUES (1);");
$node_publisher->wait_for_catchup('mysub');
$result = $node_subscriber->safe_psql('postgres', "SELECT count(*) from pg_tables where tablename = 'tmp';");
is($result, qq(1), 'CREATE UNLOGGED TABLE is replicated correctly');
$result = $node_subscriber->safe_psql('postgres', "SELECT count(*) from tmp;");
is($result, qq(0), 'inserting data to unlogged table is not replicated correctly');
$node_publisher->safe_psql('postgres', "DROP TABLE tmp");

# Test CREATE TABLE IF NOT EXISTS
$node_publisher->safe_psql('postgres', "CREATE TABLE IF NOT EXISTS tmp (id int);");
$node_publisher->safe_psql('postgres', "INSERT INTO tmp VALUES (1);");
$node_publisher->wait_for_catchup('mysub');
$result = $node_subscriber->safe_psql('postgres', "SELECT count(*) from pg_tables where tablename = 'tmp';");
is($result, qq(1), 'CREATE TABLE IF NOT EXISTS replicated');
$result = $node_subscriber->safe_psql('postgres', "SELECT count(*) from tmp;");
is($result, qq(1), 'inserting data to a table replicated');
$node_publisher->safe_psql('postgres', "DROP TABLE tmp");

# Test CREATE TABLE IF NOT EXISTS (check if we skip to create a table
# when we have the table on the subscriber in advance, and if we succeed
# in replicating changes.)
$node_subscriber->safe_psql('postgres', "CREATE TABLE tmp (id int);");
$node_publisher->safe_psql('postgres', "CREATE TABLE IF NOT EXISTS tmp (id int);");
$node_publisher->safe_psql('postgres', "INSERT INTO tmp VALUES (1);");
$node_publisher->wait_for_catchup('mysub');
$result = $node_subscriber->safe_psql('postgres', "SELECT count(*) from tmp;");
is($result, qq(1), 'CREATE TABLE IF NOT EXISTS replicated');
$node_publisher->safe_psql('postgres', "DROP TABLE tmp");

# Test CREATE TABLE IF NOT EXISTS (check if we skip to create a table
# when we have the table on the publisher, but not on the subscriber.)
$node_publisher->safe_psql('postgres', "CREATE TABLE tmp (id int);");
$node_publisher->safe_psql('postgres', "CREATE TABLE IF NOT EXISTS tmp (id int);");
$node_publisher->safe_psql('postgres', "INSERT INTO tmp VALUES (1);");
$node_publisher->wait_for_catchup('mysub');
$result = $node_subscriber->safe_psql('postgres', "SELECT count(*) from tmp;");
is($result, qq(1), 'CREATE TABLE IF NOT EXISTS replicated');
$node_publisher->safe_psql('postgres', "DROP TABLE tmp");

# Test CREATE TABLE with collate
$node_publisher->safe_psql('postgres', "CREATE TABLE tmp (name text COLLATE \"C\");");
$node_publisher->safe_psql('postgres', "INSERT INTO tmp VALUES ('foo');");
$node_publisher->wait_for_catchup('mysub');
$result = $node_subscriber->safe_psql('postgres', "SELECT collation_name FROM information_schema.columns WHERE table_name = 'tmp';");
is($result, qq(C), 'CREATE TABLE with collate replicated');
$node_publisher->safe_psql('postgres', "DROP TABLE tmp");

# Test CREATE TABLE with named constraint
$node_publisher->safe_psql('postgres', "CREATE TABLE tmp (id int CONSTRAINT \"must be bigger than 10\" CHECK (id > 10));");
$node_publisher->wait_for_catchup('mysub');
$node_subscriber->psql('postgres', "INSERT INTO tmp VALUES (1);",
					   on_error_stop => 0,
					   stderr => \$stderr,
					   stdout => \$stdout);
$stderr =~ /ERROR:  new row for relation "tmp" violates check constraint "must be bigger than 10"/
  or die "failed to replicate named constraint at creating table";
$node_publisher->safe_psql('postgres', "DROP TABLE tmp;");

# Test CREATE TABLE with various types of constraints.
# NOT NULL constraint
$node_publisher->safe_psql('postgres', "CREATE TABLE tmp (id int, name text NOT NULL);");
$node_publisher->wait_for_catchup('mysub');
$node_subscriber->psql('postgres', "INSERT INTO tmp VALUES (1);",
					   on_error_stop => 0,
					   stderr => \$stderr,
					   stdout => \$stdout);
$stderr =~ /ERROR:  null value in column "name" of relation "tmp" violates not-null constraint/
  or die "failed to replicate non null constraint at creating table";
$node_publisher->safe_psql('postgres', "DROP TABLE tmp;");

# NULL constraint
$node_publisher->safe_psql('postgres', "CREATE TABLE tmp (id int, name text NULL);");
$node_publisher->wait_for_catchup('mysub');
$node_subscriber->safe_psql('postgres', "INSERT INTO tmp VALUES (1);");
$result = $node_subscriber->safe_psql('postgres', "SELECT name IS NULL FROM tmp;");
is($result, qq(t), "CREATE TABLE with NULL constraint replicated");
$node_publisher->safe_psql('postgres', "DROP TABLE tmp");

# CHECK constraint
$node_publisher->safe_psql('postgres', "CREATE TABLE tmp (id int, product_ame text, price int CHECK (price > 0));");
$node_publisher->wait_for_catchup('mysub');
$node_subscriber->psql('postgres', "INSERT INTO tmp VALUES (1, 'foo', -100);",
					   on_error_stop => 0,
					   stderr => \$stderr,
					   stdout => \$stdout);
$stderr =~ /ERROR:  new row for relation "tmp" violates check constraint "tmp_price_check"/
  or die "failed to replicate CHECK constraint";
$node_publisher->safe_psql('postgres', "DROP TABLE tmp;");

# DEFAULT
$node_publisher->safe_psql('postgres', "CREATE TABLE tmp (id int, name text DEFAULT 'foo');");
$node_publisher->wait_for_catchup('mysub');
$node_subscriber->safe_psql('postgres', "INSERT INTO tmp VALUES (1);");
$result = $node_subscriber->safe_psql('postgres', "SELECT name from tmp;");
is($result, qq(foo), "CREATE TABLE with default value replicated");
$node_publisher->safe_psql('postgres', "DROP TABLE tmp");

# UNIQUE constraint
$node_publisher->safe_psql('postgres', "CREATE TABLE tmp (id int UNIQUE);");
$node_publisher->safe_psql('postgres', "INSERT INTO tmp VALUES (1);");
$node_publisher->wait_for_catchup('mysub');
$node_subscriber->psql('postgres', "INSERT INTO tmp VALUES (1);",
					   on_error_stop => 0,
					   stderr => \$stderr,
					   stdout => \$stdout);
$stderr =~ /ERROR:  duplicate key value violates unique constraint "tmp_id_key"/
  or die "failed to replicate constraint at creating table";
$node_publisher->safe_psql('postgres', "DROP TABLE tmp;");

# PRIMARY KEY
$node_publisher->safe_psql('postgres', "CREATE TABLE tmp (id int PRIMARY KEY, name text);");
$node_publisher->safe_psql('postgres', "INSERT INTO tmp VALUES (1, 'foo');");
$node_publisher->wait_for_catchup('mysub');
$node_subscriber->psql('postgres', "INSERT INTO tmp VALUES (1, 'bar');",
					   on_error_stop => 0,
					   stderr => \$stderr,
					   stdout => \$stdout);
$stderr =~ /ERROR:  duplicate key value violates unique constraint "tmp_pkey"/
  or die "failed to replicate primary key at creating table";
$node_publisher->safe_psql('postgres', "DROP TABLE tmp;");

# EXCLUDE
$node_publisher->safe_psql('postgres', "CREATE TABLE circles (c circle, EXCLUDE USING gist (c WITH &&));");
$node_publisher->safe_psql('postgres', "INSERT INTO circles VALUES ('<(1, 1), 1>'::circle);");
$node_publisher->wait_for_catchup('mysub');
$node_subscriber->psql('postgres', "INSERT INTO circles VALUES ('<(1, 1), 1>'::circle);",
					   on_error_stop => 0,
					   stderr => \$stderr,
					   stdout => \$stdout);
$stderr =~ /ERROR:  conflicting key value violates exclusion constraint "circles_c_excl"/
  or die "failed to replicate EXCLUDE at creating table";
$node_publisher->safe_psql('postgres', "DROP TABLE circles");

# REFERENCES
$node_publisher->safe_psql('postgres', "CREATE TABLE product (id int PRIMARY KEY, name text);");
$node_publisher->safe_psql('postgres', "INSERT INTO product VALUES (1, 'foo');");
$node_publisher->safe_psql('postgres', "INSERT INTO product VALUES (2, 'bar');");
$node_publisher->safe_psql('postgres', "CREATE TABLE orders (order_id int PRIMARY KEY, product_id int REFERENCES product (id))");
$node_publisher->wait_for_catchup('mysub');
$node_subscriber->psql('postgres', "INSERT INTO orders VALUES (1, 10)",
					   on_error_stop => 0,
					   stderr => \$stderr,
					   stdout => \$stdout);
$stderr =~ /ERROR:  insert or update on table "orders" violates foreign key constraint "orders_product_id_fkey"/
  or die "failed to replicate REFERENCES at creating table";
$node_publisher->safe_psql('postgres', "DROP TABLE orders");
$node_publisher->safe_psql('postgres', "DROP TABLE product");

# DEFERRABLE
$node_publisher->safe_psql('postgres', "CREATE TABLE tmp (id int PRIMARY KEY DEFERRABLE, name text);");
$node_publisher->safe_psql('postgres', "INSERT INTO tmp VALUES (1, 'foo');");
$node_publisher->safe_psql('postgres', "INSERT INTO tmp VALUES (2, 'bar');");
$node_publisher->wait_for_catchup('mysub');
# Quick check of deferrable clause
$node_subscriber->safe_psql('postgres', "UPDATE tmp SET id = id + 1;");
# Also, execute a test that should fail for INITIALLY IMMEDIATE(the default)
$node_subscriber->psql('postgres', qq(
BEGIN;
UPDATE tmp SET id = id + 1;
INSERT INTO tmp VALUES (3, 'foobar');
DELETE FROM tmp WHERE id = 3;
COMMIT;
), on_error_stop => 0, stderr => \$stderr, stdout => \$stdout);
$stderr =~ /ERROR:  duplicate key value violates unique constraint "tmp_pkey"/
  or die "failed to replicate DEFERRABLE at creating table";
$node_publisher->safe_psql('postgres', "DROP TABLE tmp;");

# NOT DEFERRABLE
$node_publisher->safe_psql('postgres', "CREATE TABLE tmp (id int PRIMARY KEY NOT DEFERRABLE, name text);");
$node_publisher->safe_psql('postgres', "INSERT INTO tmp VALUES (1, 'foo');");
$node_publisher->safe_psql('postgres', "INSERT INTO tmp VALUES (2, 'bar');");
$node_publisher->wait_for_catchup('mysub');
$node_subscriber->psql('postgres', "UPDATE tmp SET id = id + 1;",
					   on_error_stop => 0,
					   stderr => \$stderr,
					   stdout => \$stdout);
$stderr =~ /ERROR:  duplicate key value violates unique constraint "tmp_pkey"/
  or die "failed to replicate NOT DEFERRABLE at creating table";
$node_publisher->safe_psql('postgres', "DROP TABLE tmp;");

# DEFERRABLE and INITIALLY DEFERRED
$node_publisher->safe_psql('postgres', "CREATE TABLE tmp (id int PRIMARY KEY DEFERRABLE INITIALLY DEFERRED, name text);");
$node_publisher->safe_psql('postgres', "INSERT INTO tmp VALUES (1, 'foo');");
$node_publisher->safe_psql('postgres', "INSERT INTO tmp VALUES (2, 'bar');");
$node_publisher->wait_for_catchup('mysub');
# Quick check of deferrable clause
$node_subscriber->safe_psql('postgres', "UPDATE tmp SET id = id + 1;");
# Also, execute a test that should succeed for INITIALLY DEFERRED
$node_subscriber->safe_psql('postgres', qq(
BEGIN;
UPDATE tmp SET id = id + 1;
INSERT INTO tmp VALUES (3, 'foobar');
DELETE FROM tmp WHERE id = 3;
COMMIT;
));
$node_publisher->safe_psql('postgres', "DROP TABLE tmp;");

# Test CREATE TABLE with table constraint
# We will set two checks and conduct two inserts that should fail respectively.
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tmp (price int, discounted_price int, CHECK (discounted_price > 0 AND price > discounted_price));");
$node_publisher->wait_for_catchup('mysub');
$node_subscriber->psql('postgres', "INSERT INTO tmp VALUES (100, 0);",
					   on_error_stop => 0,
					   stderr => \$stderr,
					   stdout => \$stdout);
$stderr =~ /ERROR:  new row for relation "tmp" violates check constraint "tmp_check"/
  or die "failed to replicate table constraint (first condition) at creating table";
$node_subscriber->psql('postgres', "INSERT INTO tmp VALUES (50, 100);",
					   on_error_stop => 0,
					   stderr => \$stderr,
					   stdout => \$stdout);
$stderr =~ /ERROR:  new row for relation "tmp" violates check constraint "tmp_check"/
  or die "failed to replicate table constraint (second condition) at creating table";
$node_publisher->safe_psql('postgres', "DROP TABLE tmp;");

# Test CREATE TABLE WITH strorage_parameter
$node_publisher->safe_psql('postgres', "CREATE TABLE tmp (id int) WITH (fillfactor = 80);");
$node_publisher->safe_psql('postgres', "INSERT INTO tmp VALUES (1);");
$node_publisher->wait_for_catchup('mysub');
$node_subscriber->psql('postgres', "SELECT reloptions FROM pg_class WHERE relname = 'tmp';",
					   on_error_stop => 0,
					   stderr => \$stderr,
					   stdout => \$stdout);
$stdout =~ /{fillfactor=80}/
  or die "failed to replicate storage option";
$result = $node_subscriber->safe_psql('postgres', "SELECT count(*) from tmp;");
is($result, qq(1), 'CREATE TABLE with storage_parameter replicated');
$node_publisher->safe_psql('postgres', "DROP TABLE tmp;");

# Test CREATE TABLE TABLESPACE (creating a tablespace is not replicated)
# Prepare the directories for the publisher and subscriber first.
my ($basedir, $tablespace_dir);

$basedir = $node_publisher->basedir;
$tablespace_dir = "$basedir/tblspc_pub";
mkdir($tablespace_dir);
$node_publisher->safe_psql('postgres', "CREATE TABLESPACE mytblspc LOCATION '$tablespace_dir';");
$basedir = $node_subscriber->basedir;
$tablespace_dir = "$basedir/tblspc_sub";
mkdir ($tablespace_dir);
$node_subscriber->safe_psql('postgres', "CREATE TABLESPACE mytblspc LOCATION '$tablespace_dir';");

$node_publisher->safe_psql('postgres', "CREATE TABLE tmp (id int) TABLESPACE mytblspc;");
$node_publisher->safe_psql('postgres', "INSERT INTO tmp VALUES (1);");
$node_publisher->wait_for_catchup('mysub');
$result = $node_subscriber->safe_psql('postgres', "SELECT count(*) from tmp;");
is($result, qq(1), 'CREATE TABLE TABLESPACE replicated');
$node_publisher->safe_psql('postgres', "DROP TABLE tmp;");

# Test CREATE TABLE OF (creating a type is not replicated)
$node_publisher->safe_psql('postgres', "CREATE TYPE mytype AS (id int, name text, age int);");
$node_subscriber->safe_psql('postgres', "CREATE TYPE mytype AS (id int, name text, age int);");
$node_publisher->safe_psql('postgres', "CREATE TABLE tmp OF mytype;");
$node_publisher->safe_psql('postgres', "INSERT INTO tmp VALUES (1, 'bar');");
$node_publisher->wait_for_catchup('mysub');
$result = $node_subscriber->safe_psql('postgres', "SELECT count(*) from tmp;");
is($result, qq(1), 'CREATE TABLE OF replicated');
$node_publisher->safe_psql('postgres', "DROP TABLE tmp");

pass "DDL replication tests passed:";

$node_subscriber->stop;
$node_publisher->stop;

done_testing();
