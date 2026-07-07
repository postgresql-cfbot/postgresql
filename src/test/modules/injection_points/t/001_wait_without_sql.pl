# Copyright (c) 2026, PostgreSQL Global Development Group

# Exercise driving a wait injection point purely through the filesystem, with
# no SQL used for the coordination itself.  This is meant for code paths that
# run before the server can answer queries (e.g. early postmaster startup).
#
# Layout under the data directory:
#   pg_injection_points/<point>/         present  -> <point> attached as a wait
#   pg_injection_points/<point>/<pid>    present  -> that backend is parked here
# Removing the <pid> file wakes that backend.

use strict;
use warnings FATAL => 'all';

use Time::HiRes qw(usleep);

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $point = 'no-sql-wait';

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'injection_points'");

# Attach the wait point before the server is even running, without any SQL:
# just create its directory.  The module scans these at startup.
my $root = $node->data_dir . '/pg_injection_points';
my $pdir = "$root/$point";
mkdir $root or die "could not create $root: $!";
mkdir $pdir or die "could not create $pdir: $!";

$node->start;

# The module attached the point from its directory at startup; we never called
# injection_points_attach().
$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');

my $listed = $node->safe_psql('postgres',
	"SELECT count(*) FROM injection_points_list() WHERE point_name = '$point';"
);
is($listed, '1', 'wait point attached from its directory, without SQL');

# Fire the point in a background session; it must block in injection_wait().
my $bg = $node->background_psql('postgres');
$bg->query_until(
	qr/start/, qq[
\\echo start
SELECT injection_points_run('$point');
]);

# Detect that the backend is parked, purely through the filesystem: it
# publishes a file named after its PID inside the point directory.
my $waiter;
foreach my $i (1 .. 1800)
{
	if (opendir(my $dh, $pdir))
	{
		($waiter) = grep { /^\d+\z/ } readdir($dh);
		closedir($dh);
		last if defined $waiter;
	}
	usleep(100_000);
}
ok(defined $waiter, 'backend published its waiter file (filesystem-observable)');

# Wake that specific backend without any SQL: remove its waiter file.
unlink "$pdir/$waiter" or die "could not remove waiter file: $!";

# The blocked statement now finishes, proving the filesystem wakeup worked.
like($bg->query_safe('SELECT 1;'),
	qr/^1$/m, 'backend released by removing its waiter file, without SQL');
$bg->quit;

# The directory does not survive the cluster.
$node->stop;
ok(!-d $root, 'injection points directory removed at shutdown');

done_testing();
