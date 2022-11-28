# Check integrity after dump/restore with different xids
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use File::Compare;

my $tempdir = PostgreSQL::Test::Utils::tempdir;
use bigint;

my $START_VAL = 2**32;
my $MAX_VAL = 2**62;

my $ixid = $START_VAL + int(rand($MAX_VAL - $START_VAL));
my $imxid = $START_VAL + int(rand($MAX_VAL - $START_VAL));
my $imoff = $START_VAL + int(rand($MAX_VAL - $START_VAL));

# Initialize master node
my $node = PostgreSQL::Test::Cluster->new('master');
$node->init();
$node->start;

# Create a database and fill it with the pgbench data
$node->safe_psql('postgres', "CREATE DATABASE pgbench_db");
$node->command_ok(
	[ qw(pgbench --initialize --scale=2 pgbench_db) ],
	  'pgbench finished without errors');
# Dump the database (cluster the main table to put data in a determined order)
$node->safe_psql('pgbench_db', qq(
	CREATE INDEX pa_aid_idx ON pgbench_accounts (aid);
	CLUSTER pgbench_accounts USING pa_aid_idx));
$node->command_ok(
	[ "pg_dump", "-w", "--inserts", "--file=$tempdir/pgbench.sql", "pgbench_db" ],
	  'pgdump finished without errors');
$node->stop('fast');

# Initialize second node
my $node2 = PostgreSQL::Test::Cluster->new('master2');
$node2->init(extra => [ "--xid=$ixid", "--multixact-id=$imxid", "--multixact-offset=$imoff" ]);
# Disable logging of all statements to avoid log bloat during restore
$node2->append_conf('postgresql.conf', "log_statement = none");
$node2->start;

# Create a database and restore the previous dump
$node2->safe_psql('postgres', "CREATE DATABASE pgbench_db");
my $txid0 = $node2->safe_psql('pgbench_db', 'SELECT txid_current()');
print("# Initial txid_current: $txid0\n");
$node2->command_ok(["psql", "-q", "-f", "$tempdir/pgbench.sql", "pgbench_db"]);

# Dump the database and compare the dumped content with the previous one
$node2->safe_psql('pgbench_db', 'CLUSTER pgbench_accounts');
$node2->command_ok(
	[ "pg_dump", "-w", "--inserts", "--file=$tempdir/pgbench2.sql", "pgbench_db" ],
	  'pgdump finished without errors');
ok(File::Compare::compare_text("$tempdir/pgbench.sql", "$tempdir/pgbench2.sql") == 0, "no differences detected");

done_testing();