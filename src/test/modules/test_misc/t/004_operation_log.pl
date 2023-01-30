
# Copyright (c) 2022, PostgreSQL Global Development Group

# Test for operation log.
#
# Some events like
# "bootstrap", "startup", "pg_rewind", "pg_resetwal", "promoted", "pg_upgrade"
# should be registered in operation log.

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use File::Copy;

# Create and start primary node
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1);
$node_primary->append_conf('postgresql.conf', qq(wal_keep_size = 100MB));
$node_primary->start;

# Get server version
my $server_version = $node_primary->safe_psql("postgres", "SELECT current_setting('server_version_num');") + 0;
my $major_version = $server_version / 10000;
my $minor_version = $server_version % 100;

# Get primary node backup
$node_primary->backup('primary_backup');

# Initialize standby node from backup
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup($node_primary, 'primary_backup', has_streaming => 1);
$node_standby->start;

# Promote the standby
$node_standby->promote;

# Stop standby node
$node_standby->stop;

my $node_standby_pgdata  = $node_standby->data_dir;
my $node_primary_connstr = $node_primary->connstr;

# Keep a temporary postgresql.conf or it would be overwritten during the rewind.
my $tmp_folder = PostgreSQL::Test::Utils::tempdir;
copy("$node_standby_pgdata/postgresql.conf", "$tmp_folder/node_standby-postgresql.conf.tmp");

# Get "pg_rewind" event
command_ok(
	[
		'pg_rewind',
		"--source-server=$node_primary_connstr",
		"--target-pgdata=$node_standby_pgdata",
		"--debug"
	],
	'run pg_rewind');

# Move back postgresql.conf with old settings
move("$tmp_folder/node_standby-postgresql.conf.tmp", "$node_standby_pgdata/postgresql.conf");

# Start and stop standby before resetwal and upgrade
$node_standby->start;
$node_standby->stop;

# Get first "pg_resetwal" event
system_or_bail('pg_resetwal', '-f', $node_standby->data_dir);

# Get second "pg_resetwal" event
system_or_bail('pg_resetwal', '-f', $node_standby->data_dir);

# Initialize a new node for the upgrade
my $node_new = PostgreSQL::Test::Cluster->new('new');
$node_new->init;

my $bindir_new = $node_new->config_data('--bindir');
my $bindir_standby = $node_standby->config_data('--bindir');

# We want to run pg_upgrade in the build directory so that any files generated
# finish in it, like delete_old_cluster.{sh,bat}.
chdir ${PostgreSQL::Test::Utils::tmp_check};

# Run pg_upgrade
command_ok(
	[
		'pg_upgrade', '--no-sync',         '-d', $node_standby->data_dir,
		'-D',         $node_new->data_dir, '-b', $bindir_standby,
		'-B',         $bindir_new,         '-s', $node_new->host,
		'-p',         $node_standby->port, '-P', $node_new->port
	],
	'run pg_upgrade');
#
# Need to check operation log
#
sub check_event
{
	my $event_name = shift;
	my $result = shift;
	my $func_args = shift ? "sum(count), count(*)" : "count(*)";

	my $psql_stdout = $node_new->safe_psql('postgres', qq(
		SELECT
			$func_args,
			min(split_part(version,'.','1')),
			min(split_part(version,'.','2'))
		FROM
			pg_operation_log()
		WHERE
			event='$event_name'));

	is($psql_stdout, $result, 'check number of event ' . $event_name);
	return;
}
#Start new node
$node_new->start;

# Check number of event "bootstrap"
check_event('bootstrap', qq(1|1|$major_version|$minor_version), 1);

# Check number of event "startup"
check_event('startup', qq(1|$major_version|$minor_version), 0);

# Check number of event "promoted"
check_event('promoted', qq(1|1|$major_version|$minor_version), 1);

# Check number of event "pg_upgrade"
check_event('pg_upgrade', qq(1|1|$major_version|$minor_version), 1);

# Check number of event "pg_resetwal"
check_event('pg_resetwal', qq(2|1|$major_version|$minor_version), 1);

# Check number of event "pg_rewind"
check_event('pg_rewind', qq(1|1|$major_version|$minor_version), 1);

done_testing();
