
# Copyright (c) 2021, PostgreSQL Global Development Group

#
# Tests relating to PostgreSQL crash recovery and redo
#
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;
use File::Path qw(rmtree);
use Config;

plan tests => 5;

my $node = PostgresNode->new('primary');
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

# TEST 4
#
# Ensure that a missing tablespace directory during crash recovery on
# a standby is handled correctly.  The standby should finish crash
# recovery successfully because a matching drop database record is
# found in the WAL.  The following scnearios are covered:
#
# 1. Create a database against a user-defined tablespace then drop the
#    database.
#
# 2. Create a database against a user-defined tablespace then drop the
#    database and the tablespace.
#
# 3. Move a database from source tablespace to target tablespace then
#    drop the source tablespace.
#
# 4. Create a database from another database as template then drop the
#    template database.
#
#

my $node_master = PostgresNode->new('master2');
$node_master->init(allows_streaming => 1);
$node_master->start;

# Create tablespace
my $dropme_ts_master1 = TestLib::tempdir;
$dropme_ts_master1 = TestLib::perl2host($dropme_ts_master1);
my $dropme_ts_master2 = TestLib::tempdir;
$dropme_ts_master2 = TestLib::perl2host($dropme_ts_master2);
my $source_ts_master = TestLib::tempdir;
$source_ts_master = TestLib::perl2host($source_ts_master);
my $target_ts_master = TestLib::tempdir;
$target_ts_master = TestLib::perl2host($target_ts_master);

$node_master->safe_psql('postgres',
						qq[CREATE TABLESPACE dropme_ts1 location '$dropme_ts_master1';
						   CREATE TABLESPACE dropme_ts2 location '$dropme_ts_master2';
						   CREATE TABLESPACE source_ts location '$source_ts_master';
						   CREATE TABLESPACE target_ts location '$target_ts_master';
						   CREATE DATABASE template_db IS_TEMPLATE = true;]);

my $dropme_ts_standby1 = TestLib::tempdir;
$dropme_ts_standby1 = TestLib::perl2host($dropme_ts_standby1);
my $dropme_ts_standby2 = TestLib::tempdir;
$dropme_ts_standby2 = TestLib::perl2host($dropme_ts_standby2);
my $source_ts_standby = TestLib::tempdir;
$source_ts_standby = TestLib::perl2host($source_ts_standby);
my $target_ts_standby = TestLib::tempdir;
$target_ts_standby = TestLib::perl2host($target_ts_standby);

# Take backup
my $backup_name = 'my_backup';
my $ts_mapping = [ "--tablespace-mapping=$dropme_ts_master1=$dropme_ts_standby1",
  "--tablespace-mapping=$dropme_ts_master2=$dropme_ts_standby2",
  "--tablespace-mapping=$source_ts_master=$source_ts_standby",
  "--tablespace-mapping=$target_ts_master=$target_ts_standby" ];
$node_master->backup($backup_name, backup_options => $ts_mapping);

my $node_standby = PostgresNode->new('standby2');
$node_standby->init_from_backup($node_master, $backup_name, has_streaming => 1);
$node_standby->start;

# Make sure connection is made
$node_master->poll_query_until(
	'postgres', 'SELECT count(*) = 1 FROM pg_stat_replication');

# Make sure to perform restartpoint after tablespace creation
$node_master->wait_for_catchup($node_standby, 'replay',
							   $node_master->lsn('replay'));
$node_standby->safe_psql('postgres', 'CHECKPOINT');

# Do immediate shutdown just after a sequence of CREAT DATABASE / DROP
# DATABASE / DROP TABLESPACE. This causes CREATE DATABASE WAL records
# to be applied to already-removed directories.
$node_master->safe_psql('postgres',
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
$node_master->wait_for_catchup($node_standby, 'replay',
							   $node_master->lsn('replay'));
$node_standby->stop('immediate');

# Should restart ignoring directory creation error.
is($node_standby->start(fail_ok => 1), 1);

# TEST 5
#
# Ensure that a missing tablespace directory during create database
# replay immediately causes panic if the standby has already reached
# consistent state (archive recovery is in progress).

$node_master = PostgresNode->new('master3');
$node_master->init(allows_streaming => 1);
$node_master->start;

# Create tablespace
my $ts_master = TestLib::tempdir;
$ts_master = TestLib::perl2host($ts_master);
$node_master->safe_psql('postgres', "CREATE TABLESPACE ts1 LOCATION '$ts_master'");
$node_master->safe_psql('postgres', "CREATE DATABASE db1 TABLESPACE ts1");

my $ts_standby = TestLib::tempdir("standby");
$ts_standby = TestLib::perl2host($ts_standby);

# Take backup
$backup_name = 'my_backup';
$node_master->backup($backup_name,
					 backup_options =>
					   [ "--tablespace-mapping=$ts_master=$ts_standby" ]);
$node_standby = PostgresNode->new('standby3');
$node_standby->init_from_backup($node_master, $backup_name, has_streaming => 1);
$node_standby->start;

# Make sure standby reached consistency and starts accepting connections
$node_standby->poll_query_until('postgres', 'SELECT 1', '1');

# Remove standby tablespace directory so it will be missing when
# replay resumes.
#
# The tablespace mapping is lost when the standby node is initialized
# from basebackup because RecursiveCopy::copypath creates a new temp
# directory for each tablspace symlink found in backup.  We must
# obtain the correct tablespace directory by querying standby.
$ts_standby = $node_standby->safe_psql(
	'postgres',
	"select pg_tablespace_location(oid) from pg_tablespace where spcname = 'ts1'");
rmtree($ts_standby);

# Create a database in the tablespace and a table in default tablespace
$node_master->safe_psql('postgres',
						q[CREATE TABLE should_not_replay_insertion(a int);
						  CREATE DATABASE db2 WITH TABLESPACE ts1;
						  INSERT INTO should_not_replay_insertion VALUES (1);]);

# Standby should fail and should not silently skip replaying the wal
if ($node_master->poll_query_until(
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
