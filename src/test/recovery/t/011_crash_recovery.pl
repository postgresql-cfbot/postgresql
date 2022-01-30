
# Copyright (c) 2021-2022, PostgreSQL Global Development Group

#
# Tests relating to PostgreSQL crash recovery and redo
#
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Config;

plan tests => 5;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init(allows_streaming => 1);
$node->start;

my ($stdin, $stdout, $stderr) = ('', '', '');

# Ensure that pg_xact_status reports 'aborted' for xacts
# that were in-progress during crash. To do that, we need
# an xact to be in-progress when we crash and we need to know
# its xid.
my $tx = IPC::Run::start(
	[
		'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-f', '-', '-d',
		$node->connstr('postgres')
	],
	'<',
	\$stdin,
	'>',
	\$stdout,
	'2>',
	\$stderr);
$stdin .= q[
BEGIN;
CREATE TABLE mine(x integer);
SELECT pg_current_xact_id();
];
$tx->pump until $stdout =~ /[[:digit:]]+[\r\n]$/;

# Status should be in-progress
my $xid = $stdout;
chomp($xid);

is($node->safe_psql('postgres', qq[SELECT pg_xact_status('$xid');]),
	'in progress', 'own xid is in-progress');

# Crash and restart the postmaster
$node->stop('immediate');
$node->start;

# Make sure we really got a new xid
cmp_ok($node->safe_psql('postgres', 'SELECT pg_current_xact_id()'),
	'>', $xid, 'new xid after restart is greater');

# and make sure we show the in-progress xact as aborted
is($node->safe_psql('postgres', qq[SELECT pg_xact_status('$xid');]),
	'aborted', 'xid is aborted after crash');

$stdin .= "\\q\n";
$tx->finish;    # wait for psql to quit gracefully

my $node_primary = PostgreSQL::Test::Cluster->new('primary2');
$node_primary->init(allows_streaming => 1);
$node_primary->start;
my $dropme_ts_primary1 = $node_primary->new_tablespace('dropme_ts1');
my $dropme_ts_primary2 = $node_primary->new_tablespace('dropme_ts2');
my $soruce_ts_primary = $node_primary->new_tablespace('source_ts');
my $target_ts_primary = $node_primary->new_tablespace('target_ts');

$node_primary->psql('postgres',
qq[
	CREATE TABLESPACE dropme_ts1 LOCATION '$dropme_ts_primary1';
	CREATE TABLESPACE dropme_ts2 LOCATION '$dropme_ts_primary2';
	CREATE TABLESPACE source_ts  LOCATION '$soruce_ts_primary';
	CREATE TABLESPACE target_ts  LOCATION '$target_ts_primary';
    CREATE DATABASE template_db IS_TEMPLATE = true;
]);
my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

my $node_standby = PostgreSQL::Test::Cluster->new('standby2');
$node_standby->init_from_backup($node_primary, $backup_name, has_streaming => 1);
$node_standby->start;

# Make sure connection is made
$node_primary->poll_query_until(
	'postgres', 'SELECT count(*) = 1 FROM pg_stat_replication');

$node_standby->safe_psql('postgres', 'CHECKPOINT');

# Do immediate shutdown just after a sequence of CREAT DATABASE / DROP
# DATABASE / DROP TABLESPACE. This causes CREATE DATABASE WAL records
# to be applied to already-removed directories.
$node_primary->safe_psql('postgres',
						q[CREATE DATABASE dropme_db1 WITH TABLESPACE dropme_ts1;
						  CREATE DATABASE dropme_db2 WITH TABLESPACE dropme_ts2;
						  CREATE DATABASE moveme_db TABLESPACE source_ts;
						  ALTER DATABASE moveme_db SET TABLESPACE target_ts;
						  CREATE DATABASE newdb TEMPLATE template_db;
						  ALTER DATABASE template_db IS_TEMPLATE = false;
						  DROP DATABASE dropme_db1;
						  DROP DATABASE dropme_db2; DROP TABLESPACE dropme_ts2;
						  DROP TABLESPACE source_ts;
						  DROP DATABASE template_db;]);

$node_primary->wait_for_catchup($node_standby, 'replay',
							   $node_primary->lsn('replay'));
$node_standby->stop('immediate');

# Should restart ignoring directory creation error.
is($node_standby->start(fail_ok => 1), 1);


# TEST 5
#
# Ensure that a missing tablespace directory during create database
# replay immediately causes panic if the standby has already reached
# consistent state (archive recovery is in progress).

$node_primary = PostgreSQL::Test::Cluster->new('primary3');
$node_primary->init(allows_streaming => 1);
$node_primary->start;

# Create tablespace
my $ts_primary = $node_primary->new_tablespace('dropme_ts1');
$node_primary->safe_psql('postgres',
						 "CREATE TABLESPACE ts1 LOCATION '$ts_primary'");
$node_primary->safe_psql('postgres', "CREATE DATABASE db1 TABLESPACE ts1");

# Take backup
$backup_name = 'my_backup';
$node_primary->backup($backup_name);
$node_standby = PostgreSQL::Test::Cluster->new('standby3');
$node_standby->init_from_backup($node_primary, $backup_name, has_streaming => 1);
$node_standby->start;

# Make sure standby reached consistency and starts accepting connections
$node_standby->poll_query_until('postgres', 'SELECT 1', '1');

# Remove standby tablespace directory so it will be missing when
# replay resumes.
File::Path::rmtree($node_standby->tablespace_dir('dropme_ts1'));

# Create a database in the tablespace and a table in default tablespace
$node_primary->safe_psql('postgres',
						q[CREATE TABLE should_not_replay_insertion(a int);
						  CREATE DATABASE db2 WITH TABLESPACE ts1;
						  INSERT INTO should_not_replay_insertion VALUES (1);]);

# Standby should fail and should not silently skip replaying the wal
if ($node_primary->poll_query_until(
		'postgres',
		'SELECT count(*) = 0 FROM pg_stat_replication',
		't') == 1)
{
	pass('standby failed as expected');
	# We know that the standby has failed.  Setting its pid to
	# undefined avoids error when PostgreNode module tries to stop the
	# standby node as part of tear_down sequence.
	$node_standby->{_pid} = undef;
}
else
{
	fail('standby did not fail within 5 seconds');
}
