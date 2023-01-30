
# Copyright (c) 2021-2023, PostgreSQL Global Development Group

# Tests for various bugs found over time
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Bug #15114

# The bug was that determining which columns are part of the replica
# identity index using RelationGetIndexAttrBitmap() would run
# eval_const_expressions() on index expressions and predicates across
# all indexes of the table, which in turn might require a snapshot,
# but there wasn't one set, so it crashes.  There were actually two
# separate bugs, one on the publisher and one on the subscriber.  The
# fix was to avoid the constant expressions simplification in
# RelationGetIndexAttrBitmap(), so it's safe to call in more contexts.

my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->start;

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';

$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab1 (a int PRIMARY KEY, b int)");

$node_publisher->safe_psql('postgres',
	"CREATE FUNCTION double(x int) RETURNS int IMMUTABLE LANGUAGE SQL AS 'select x * 2'"
);

# an index with a predicate that lends itself to constant expressions
# evaluation
$node_publisher->safe_psql('postgres',
	"CREATE INDEX ON tab1 (b) WHERE a > double(1)");

# and the same setup on the subscriber
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab1 (a int PRIMARY KEY, b int)");

$node_subscriber->safe_psql('postgres',
	"CREATE FUNCTION double(x int) RETURNS int IMMUTABLE LANGUAGE SQL AS 'select x * 2'"
);

$node_subscriber->safe_psql('postgres',
	"CREATE INDEX ON tab1 (b) WHERE a > double(1)");

$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION pub1 FOR ALL TABLES");

$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub1 CONNECTION '$publisher_connstr' PUBLICATION pub1"
);

$node_publisher->wait_for_catchup('sub1');

# This would crash, first on the publisher, and then (if the publisher
# is fixed) on the subscriber.
$node_publisher->safe_psql('postgres', "INSERT INTO tab1 VALUES (1, 2)");

$node_publisher->wait_for_catchup('sub1');

pass('index predicates do not cause crash');

# https://www.postgresql.org/message-id/flat/adf0452f-8c6b-7def-d35e-ab516c80088e%40inbox.ru

# The bug was that when a syntax error occurred in a SQL-language or PL/pgSQL-language
# CREATE FUNCTION or DO command executed in a logical replication worker,
# we'd suffer a null pointer dereference or assertion failure.

$node_subscriber->safe_psql('postgres', q{
	create or replace procedure rebuild_test(
	) as
	$body$
	declare
		l_code  text:= E'create or replace function public.test_selector(
	) returns setof public.tab1 as
	\$body\$
		select * from  error_name
	\$body\$ language sql;';
	begin
		execute l_code;
		perform public.test_selector();
	end
	$body$ language plpgsql;
	create or replace function test_trg()
	returns trigger as
	$body$
		declare
		begin
			call public.rebuild_test();
			return NULL;
		end
	$body$ language plpgsql;
	create trigger test_trigger after insert on tab1 for each row
								execute function test_trg();
	alter table tab1 enable replica trigger test_trigger;
});

# This command will cause a crash on the replica if that bug hasn't been fixed
$node_publisher->safe_psql('postgres', "INSERT INTO tab1 VALUES (3, 4)");

my $result = $node_subscriber->wait_for_log(
	"ERROR:  relation \"error_name\" does not exist at character"
);

ok($result,
	"ERROR: Logical decoding doesn't fail on function error");

# We'll re-use these nodes below, so drop their replication state.
$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION sub1");
$node_publisher->safe_psql('postgres', "DROP PUBLICATION pub1");
# Drop the tables too.
$node_publisher->safe_psql('postgres', "DROP TABLE tab1");

$node_publisher->stop('fast');
$node_subscriber->stop('fast');


# Handling of temporary and unlogged tables with FOR ALL TABLES publications

# If a FOR ALL TABLES publication exists, temporary and unlogged
# tables are ignored for publishing changes.  The bug was that we
# would still check in that case that such a table has a replica
# identity set before accepting updates.  If it did not it would cause
# an error when an update was attempted.

$node_publisher->rotate_logfile();
$node_publisher->start();

# Although we don't use node_subscriber in this test, keep its logfile
# name in step with node_publisher for later tests.
$node_subscriber->rotate_logfile();

$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION pub FOR ALL TABLES");

is( $node_publisher->psql(
		'postgres',
		"CREATE TEMPORARY TABLE tt1 AS SELECT 1 AS a; UPDATE tt1 SET a = 2;"),
	0,
	'update to temporary table without replica identity with FOR ALL TABLES publication'
);

is( $node_publisher->psql(
		'postgres',
		"CREATE UNLOGGED TABLE tu1 AS SELECT 1 AS a; UPDATE tu1 SET a = 2;"),
	0,
	'update to unlogged table without replica identity with FOR ALL TABLES publication'
);

# Again, drop replication state but not tables.
$node_publisher->safe_psql('postgres', "DROP PUBLICATION pub");

$node_publisher->stop('fast');


# Bug #16643 - https://postgr.es/m/16643-eaadeb2a1a58d28c@postgresql.org
#
# Initial sync doesn't complete; the protocol was not being followed per
# expectations after commit 07082b08cc5d.
my $node_twoways = PostgreSQL::Test::Cluster->new('twoways');
$node_twoways->init(allows_streaming => 'logical');
$node_twoways->start;
for my $db (qw(d1 d2))
{
	$node_twoways->safe_psql('postgres', "CREATE DATABASE $db");
	$node_twoways->safe_psql($db,        "CREATE TABLE t (f int)");
	$node_twoways->safe_psql($db,        "CREATE TABLE t2 (f int)");
}

my $rows = 3000;
$node_twoways->safe_psql(
	'd1', qq{
	INSERT INTO t SELECT * FROM generate_series(1, $rows);
	INSERT INTO t2 SELECT * FROM generate_series(1, $rows);
	CREATE PUBLICATION testpub FOR TABLE t;
	SELECT pg_create_logical_replication_slot('testslot', 'pgoutput');
	});

$node_twoways->safe_psql('d2',
	    "CREATE SUBSCRIPTION testsub CONNECTION \$\$"
	  . $node_twoways->connstr('d1')
	  . "\$\$ PUBLICATION testpub WITH (create_slot=false, "
	  . "slot_name='testslot')");
$node_twoways->safe_psql(
	'd1', qq{
	INSERT INTO t SELECT * FROM generate_series(1, $rows);
	INSERT INTO t2 SELECT * FROM generate_series(1, $rows);
	});
$node_twoways->safe_psql('d1', 'ALTER PUBLICATION testpub ADD TABLE t2');
$node_twoways->safe_psql('d2',
	'ALTER SUBSCRIPTION testsub REFRESH PUBLICATION');

# We cannot rely solely on wait_for_catchup() here; it isn't sufficient
# when tablesync workers might still be running. So in addition to that,
# verify that tables are synced.
$node_twoways->wait_for_subscription_sync($node_twoways, 'testsub', 'd2');

is($node_twoways->safe_psql('d2', "SELECT count(f) FROM t"),
	$rows * 2, "2x$rows rows in t");
is($node_twoways->safe_psql('d2', "SELECT count(f) FROM t2"),
	$rows * 2, "2x$rows rows in t2");

# Verify table data is synced with cascaded replication setup. This is mainly
# to test whether the data written by tablesync worker gets replicated.
my $node_pub = PostgreSQL::Test::Cluster->new('testpublisher1');
$node_pub->init(allows_streaming => 'logical');
$node_pub->start;

my $node_pub_sub = PostgreSQL::Test::Cluster->new('testpublisher_subscriber');
$node_pub_sub->init(allows_streaming => 'logical');
$node_pub_sub->start;

my $node_sub = PostgreSQL::Test::Cluster->new('testsubscriber1');
$node_sub->init(allows_streaming => 'logical');
$node_sub->start;

# Create the tables in all nodes.
$node_pub->safe_psql('postgres', "CREATE TABLE tab1 (a int)");
$node_pub_sub->safe_psql('postgres', "CREATE TABLE tab1 (a int)");
$node_sub->safe_psql('postgres', "CREATE TABLE tab1 (a int)");

# Create a cascaded replication setup like:
# N1 - Create publication testpub1.
# N2 - Create publication testpub2 and also include subscriber which subscribes
#      to testpub1.
# N3 - Create subscription testsub2 subscribes to testpub2.
#
# Note that subscription on N3 needs to be created before subscription on N2 to
# test whether the data written by tablesync worker of N2 gets replicated.
$node_pub->safe_psql('postgres',
	"CREATE PUBLICATION testpub1 FOR TABLE tab1");

$node_pub_sub->safe_psql('postgres',
	"CREATE PUBLICATION testpub2 FOR TABLE tab1");

my $publisher1_connstr = $node_pub->connstr . ' dbname=postgres';
my $publisher2_connstr = $node_pub_sub->connstr . ' dbname=postgres';

$node_sub->safe_psql('postgres',
	"CREATE SUBSCRIPTION testsub2 CONNECTION '$publisher2_connstr' PUBLICATION testpub2"
);

$node_pub_sub->safe_psql('postgres',
	"CREATE SUBSCRIPTION testsub1 CONNECTION '$publisher1_connstr' PUBLICATION testpub1"
);

$node_pub->safe_psql('postgres',
	"INSERT INTO tab1 values(generate_series(1,10))");

# Verify that the data is cascaded from testpub1 to testsub1 and further from
# testpub2 (which had testsub1) to testsub2.
$node_pub->wait_for_catchup('testsub1');
$node_pub_sub->wait_for_catchup('testsub2');

# Drop subscriptions as we don't need them anymore
$node_pub_sub->safe_psql('postgres', "DROP SUBSCRIPTION testsub1");
$node_sub->safe_psql('postgres', "DROP SUBSCRIPTION testsub2");

# Drop publications as we don't need them anymore
$node_pub->safe_psql('postgres', "DROP PUBLICATION testpub1");
$node_pub_sub->safe_psql('postgres', "DROP PUBLICATION testpub2");

# Clean up the tables on both publisher and subscriber as we don't need them
$node_pub->safe_psql('postgres', "DROP TABLE tab1");
$node_pub_sub->safe_psql('postgres', "DROP TABLE tab1");
$node_sub->safe_psql('postgres', "DROP TABLE tab1");

$node_pub->stop('fast');
$node_pub_sub->stop('fast');
$node_sub->stop('fast');

# https://postgr.es/m/OS0PR01MB61133CA11630DAE45BC6AD95FB939%40OS0PR01MB6113.jpnprd01.prod.outlook.com

# The bug was that when changing the REPLICA IDENTITY INDEX to another one, the
# target table's relcache was not being invalidated. This leads to skipping
# UPDATE/DELETE operations during apply on the subscriber side as the columns
# required to search corresponding rows won't get logged.

$node_publisher->rotate_logfile();
$node_publisher->start();

$node_subscriber->rotate_logfile();
$node_subscriber->start();

$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab_replidentity_index(a int not null, b int not null)");
$node_publisher->safe_psql('postgres',
	"CREATE UNIQUE INDEX idx_replidentity_index_a ON tab_replidentity_index(a)"
);
$node_publisher->safe_psql('postgres',
	"CREATE UNIQUE INDEX idx_replidentity_index_b ON tab_replidentity_index(b)"
);

# use index idx_replidentity_index_a as REPLICA IDENTITY on publisher.
$node_publisher->safe_psql('postgres',
	"ALTER TABLE tab_replidentity_index REPLICA IDENTITY USING INDEX idx_replidentity_index_a"
);

$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_replidentity_index VALUES(1, 1),(2, 2)");

$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab_replidentity_index(a int not null, b int not null)");
$node_subscriber->safe_psql('postgres',
	"CREATE UNIQUE INDEX idx_replidentity_index_a ON tab_replidentity_index(a)"
);
$node_subscriber->safe_psql('postgres',
	"CREATE UNIQUE INDEX idx_replidentity_index_b ON tab_replidentity_index(b)"
);
# use index idx_replidentity_index_b as REPLICA IDENTITY on subscriber because
# it reflects the future scenario we are testing: changing REPLICA IDENTITY
# INDEX.
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE tab_replidentity_index REPLICA IDENTITY USING INDEX idx_replidentity_index_b"
);

$publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub FOR TABLE tab_replidentity_index");
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub"
);

# Wait for initial table sync to finish
$node_subscriber->wait_for_subscription_sync($node_publisher, 'tap_sub');

is( $node_subscriber->safe_psql(
		'postgres', "SELECT * FROM tab_replidentity_index"),
	qq(1|1
2|2),
	"check initial data on subscriber");

# Set REPLICA IDENTITY to idx_replidentity_index_b on publisher, then run UPDATE and DELETE.
$node_publisher->safe_psql(
	'postgres', qq[
	ALTER TABLE tab_replidentity_index REPLICA IDENTITY USING INDEX idx_replidentity_index_b;
	UPDATE tab_replidentity_index SET a = -a WHERE a = 1;
	DELETE FROM tab_replidentity_index WHERE a = 2;
]);

$node_publisher->wait_for_catchup('tap_sub');
is( $node_subscriber->safe_psql(
		'postgres', "SELECT * FROM tab_replidentity_index"),
	qq(-1|1),
	"update works with REPLICA IDENTITY");

# Clean up
$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub");
$node_publisher->safe_psql('postgres', "DROP PUBLICATION tap_pub");
$node_publisher->safe_psql('postgres', "DROP TABLE tab_replidentity_index");
$node_subscriber->safe_psql('postgres', "DROP TABLE tab_replidentity_index");

# Test schema invalidation by renaming the schema

# Create tables on publisher
$node_publisher->safe_psql('postgres', "CREATE SCHEMA sch1");
$node_publisher->safe_psql('postgres', "CREATE TABLE sch1.t1 (c1 int)");

# Create tables on subscriber
$node_subscriber->safe_psql('postgres', "CREATE SCHEMA sch1");
$node_subscriber->safe_psql('postgres', "CREATE TABLE sch1.t1 (c1 int)");
$node_subscriber->safe_psql('postgres', "CREATE SCHEMA sch2");
$node_subscriber->safe_psql('postgres', "CREATE TABLE sch2.t1 (c1 int)");

# Setup logical replication that will cover t1 under both schema names
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub_sch FOR ALL TABLES");
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub_sch CONNECTION '$publisher_connstr' PUBLICATION tap_pub_sch"
);

# Wait for initial table sync to finish
$node_subscriber->wait_for_subscription_sync($node_publisher, 'tap_sub_sch');

# Check what happens to data inserted before and after schema rename
$node_publisher->safe_psql(
	'postgres',
	"begin;
insert into sch1.t1 values(1);
alter schema sch1 rename to sch2;
create schema sch1;
create table sch1.t1(c1 int);
insert into sch1.t1 values(2);
insert into sch2.t1 values(3);
commit;");

$node_subscriber->wait_for_subscription_sync($node_publisher, 'tap_sub_sch');

# Subscriber's sch1.t1 should receive the row inserted into the new sch1.t1,
# but not the row inserted into the old sch1.t1 post-rename.
$result = $node_subscriber->safe_psql('postgres', "SELECT * FROM sch1.t1");
is( $result, qq(1
2), 'check data in subscriber sch1.t1 after schema rename');

# Subscriber's sch2.t1 won't have gotten anything yet ...
$result = $node_subscriber->safe_psql('postgres', "SELECT * FROM sch2.t1");
is($result, '', 'no data yet in subscriber sch2.t1 after schema rename');

# ... but it should show up after REFRESH.
$node_subscriber->safe_psql('postgres',
	'ALTER SUBSCRIPTION tap_sub_sch REFRESH PUBLICATION');

$node_subscriber->wait_for_subscription_sync($node_publisher, 'tap_sub_sch');

$result = $node_subscriber->safe_psql('postgres', "SELECT * FROM sch2.t1");
is( $result, qq(1
3), 'check data in subscriber sch2.t1 after schema rename');

$node_publisher->stop('fast');
$node_subscriber->stop('fast');

done_testing();
