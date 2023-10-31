# Copyright (c) 2023, PostgreSQL Global Development Group

#
# Test using a standby server as the subscriber.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node_p;
my $node_f;
my $node_s;
my $result;

# Set up node P as primary
$node_p = PostgreSQL::Test::Cluster->new('node_p');
$node_p->init(allows_streaming => 'logical');
$node_p->start;

# Set up node F as about-to-fail node
$node_f = PostgreSQL::Test::Cluster->new('node_f');
$node_f->init(allows_streaming => 'logical');
$node_f->start;

# Create databases
# Create a test table and insert a row in primary server
$node_p->safe_psql(
	'postgres', q(
	CREATE DATABASE pg1;
	CREATE DATABASE pg2;
));
$node_p->safe_psql('pg1', "CREATE TABLE tbl1 (a text)");
$node_p->safe_psql('pg1', "INSERT INTO tbl1 VALUES('first row')");
$node_p->safe_psql('pg2', "CREATE TABLE tbl2 (a text)");

# Set up node S as standby linking to node P
$node_p->backup('backup_1');
$node_s = PostgreSQL::Test::Cluster->new('node_s');
$node_s->init_from_backup($node_p, 'backup_1', has_streaming => 1);
$node_s->set_standby_mode();
$node_s->start;

# Insert another row on P and wait standby S to catch up
$node_p->safe_psql('pg1', "INSERT INTO tbl1 VALUES('second row')");
$node_p->wait_for_replay_catchup($node_s);

# Run pg_subscriber on about-to-fail node (F)
command_fails(
	[
		'pg_subscriber', "--verbose",
		"--pgdata", $node_f->data_dir,
		"--publisher-conninfo", $node_p->connstr('pg1'),
		"--subscriber-conninfo", $node_f->connstr('pg1'),
		"--database", 'pg1',
		"--database", 'pg2'
	],
	'subscriber data directory is not a copy of the source database cluster');

# Run pg_subscriber on node S
command_ok(
	[
		'pg_subscriber', "--verbose",
		"--pgdata", $node_s->data_dir,
		"--publisher-conninfo", $node_p->connstr('pg1'),
		"--subscriber-conninfo", $node_s->connstr('pg1'),
		"--database", 'pg1',
		"--database", 'pg2'
	],
	'run pg_subscriber on node S');

# Insert rows on P
$node_p->safe_psql('pg1', "INSERT INTO tbl1 VALUES('third row')");
$node_p->safe_psql('pg2', "INSERT INTO tbl2 VALUES('row 1')");

# PID sets to undefined because subscriber was stopped behind the scenes.
# Start subscriber
$node_s->{_pid} = undef;
$node_s->start;

# Get subscription names
$result = $node_s->safe_psql(
	'postgres', qq(
	SELECT subname FROM pg_subscription WHERE subname ~ '^pg_subscriber_'
));
my @subnames = split("\n", $result);

# Wait subscriber to catch up
$node_s->wait_for_subscription_sync($node_p, $subnames[0]);
$node_s->wait_for_subscription_sync($node_p, $subnames[1]);

# Check result on database pg1
$result = $node_s->safe_psql('pg1', "SELECT * FROM tbl1");
is( $result, qq(first row
second row
third row),
	'logical replication works on database pg1');

# Check result on database pg2
$result = $node_s->safe_psql('pg2', "SELECT * FROM tbl2");
is( $result, qq(row 1),
	'logical replication works on database pg2');

# Different system identifier?
my $sysid_p = $node_p->safe_psql('postgres', "SELECT system_identifier FROM pg_control_system()");
my $sysid_s = $node_s->safe_psql('postgres', "SELECT system_identifier FROM pg_control_system()");
ok($sysid_p != $sysid_s, 'system identifier was changed');

# clean up
$node_p->teardown_node;
$node_s->teardown_node;

done_testing();
