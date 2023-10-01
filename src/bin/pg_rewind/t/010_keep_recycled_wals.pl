
# Copyright (c) 2021-2023, PostgreSQL Global Development Group

#
# Test situation where a target data directory contains
# WAL files that were already recycled by the new primary.

use strict;
use warnings;
use PostgreSQL::Test::Utils;
use Test::More;

use FindBin;
use lib $FindBin::RealBin;

use RewindTest;

setup_cluster;
$node_primary->enable_archiving;
start_primary();

create_standby;
$node_standby->enable_restoring($node_primary, 0);
$node_standby->reload;

primary_psql("CHECKPOINT");  # last common checkpoint

# primary can't archive anymore
my $false = "$^X -e 'exit(1)'";
$node_primary->append_conf(
	'postgresql.conf', qq(
archive_command = '$false'
));
$node_primary->reload;

# advance WAL on the primary; WAL segment will never make it to the archive
primary_psql("create table t(a int)");
primary_psql("insert into t values(0)");
primary_psql("select pg_switch_wal()");

promote_standby;

# new primary loses diverging WAL segment
standby_psql("insert into t values(0)");
standby_psql("select pg_switch_wal()");

$node_standby->stop;
$node_primary->stop;

my ($stdout, $stderr) = run_command(
	[
		'pg_rewind', '--debug',
		'--source-pgdata', $node_standby->data_dir,
		'--target-pgdata', $node_primary->data_dir,
		'--no-sync',
	]);

like(
	$stderr,
	qr/Not removing pg_wal.* because it is required for recovery/,
	"some WAL files were skipped");

done_testing();
