# To test successful data directory creation with a additional feature, first
# try to elaborate the "successful creation" test instead of adding a test.
# Successful initdb consumes much time and I/O.

use strict;
use warnings;
use Fcntl ':mode';
use File::stat qw{lstat};
use PostgresNode;
use TestLib;
use Test::More tests => 18;

my $tempdir = TestLib::tempdir;
my $xlogdir = "$tempdir/pgxlog";
my $datadir = "$tempdir/data";

program_help_ok('initdb');
program_version_ok('initdb');
program_options_handling_ok('initdb');

command_fails([ 'initdb', '-S', "$tempdir/nonexistent" ],
	'sync missing data directory');

mkdir $xlogdir;
mkdir "$xlogdir/lost+found";
command_fails(
	[ 'initdb', '-X', $xlogdir, $datadir ],
	'existing nonempty xlog directory');
rmdir "$xlogdir/lost+found";
command_fails(
	[ 'initdb', '-X', 'pgxlog', $datadir ],
	'relative xlog directory not allowed');

command_fails(
	[ 'initdb', '-U', 'pg_test', $datadir ],
	'role names cannot begin with "pg_"');

mkdir $datadir;

# make sure we run one successful test without a TZ setting so we test
# initdb's time zone setting code
{

	# delete local only works from perl 5.12, so use the older way to do this
	local (%ENV) = %ENV;
	delete $ENV{TZ};

	command_ok([ 'initdb', '-N', '-T', 'german', '-X', $xlogdir, $datadir ],
		'successful creation');

	ok(check_pg_data_perm($datadir, 0));
}
command_ok([ 'initdb', '-S', $datadir ], 'sync only');
command_fails([ 'initdb', $datadir ], 'existing data directory');

# Init a new db with group access
my $datadir_group = "$tempdir/data_group";

command_ok(
	[ 'initdb', '-g', $datadir_group ],
	'successful creation with group access');

ok(check_pg_data_perm($datadir_group, 1));
