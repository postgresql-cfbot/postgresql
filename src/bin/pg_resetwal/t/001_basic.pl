use strict;
use warnings;

use Config;
use PostgresNode;
use TestLib;
use Test::More tests => 16;

my $tempdir       = TestLib::tempdir;
my $tempdir_short = TestLib::tempdir_short;

program_help_ok('pg_resetwal');
program_version_ok('pg_resetwal');
program_options_handling_ok('pg_resetwal');

# Initialize node without replication settings
my $node = get_new_node('main');
my $pgdata = $node->data_dir;
my $pgwal = "$pgdata/pg_wal";
$node->init;
$node->start;

# Remove WAL from pg_wal and make sure it gets rebuilt
$node->stop;

unlink("$pgwal/000000010000000000000001") == 1
	or die("unable to remove 000000010000000000000001");

is_deeply(
	[sort(slurp_dir($pgwal))], [sort(qw(. .. archive_status))], 'no WAL');

$node->command_ok(['pg_resetwal', '-D', $pgdata], 'recreate pg_wal');

is_deeply(
	[sort(slurp_dir($pgwal))],
	[sort(qw(. .. archive_status 000000010000000000000002))],
	'WAL recreated');

ok(check_pg_data_perm($pgdata, 0), 'check PGDATA permissions');

$node->start;

# Reset to specific WAL segment.  Also enable group access to make sure files
# and directories are created with group permissions.
$node->stop;

command_ok(
	['chmod', "-R", 'g+rX', "$pgdata"],
	'add group perms to PGDATA');

$node->command_ok(
	['pg_resetwal', '-l', '000000070000000700000007', '-D', $pgdata],
	'set to specific WAL');

is_deeply(
	[sort(slurp_dir($pgwal))],
	[sort(qw(. .. archive_status 000000070000000700000007))],
	'WAL recreated');

ok(check_pg_data_perm($pgdata, 1), 'check PGDATA permissions');

$node->start;
