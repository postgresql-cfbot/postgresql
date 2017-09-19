# Set of tests for pg_upgrade.
use strict;
use warnings;
use Cwd;
use Config;
use File::Basename;
use IPC::Run;
use PostgresNode;
use TestLib;
use Test::More tests => 9;

program_help_ok('pg_upgrade');
program_version_ok('pg_upgrade');
program_options_handling_ok('pg_upgrade');

# Generate a database with a name made of a range of ASCII characters.
sub generate_db
{
	my ($node, $from_char, $to_char) = @_;

	my $dbname = '';
	for my $i ($from_char .. $to_char)
	{
		next if $i == 7 || $i == 10 || $i == 13;	# skip BEL, LF, and CR
		$dbname = $dbname . sprintf('%c', $i);
	}
	$node->run_log([ 'createdb', '--port', $node->port, $dbname ]);
}

my $startdir = getcwd();

# From now on, the test of pg_upgrade consists in setting up an instance
# on which regression tests are run. This is the source instance used
# for the upgrade. Then a new, fresh instance is created, and is used
# as the target instance for the upgrade. Before running an upgrade a
# logical dump of the old instance is taken, and a second logical dump
# of the new instance is taken after the upgrade. The upgrade test
# passes if there are no differences after running pg_upgrade.

# Temporary location for dumps taken
my $tempdir = TestLib::tempdir;

# Initialize node to upgrade
my $oldnode = get_new_node('old_node');
$oldnode->init;
$oldnode->start;

# Creating databases with names covering most ASCII bytes
generate_db($oldnode, 1,  45);
generate_db($oldnode, 46, 90);
generate_db($oldnode, 91, 127);

# Run regression tests on the old instance
chdir dirname($ENV{PG_REGRESS});
$oldnode->run_log([ 'createdb', '--port', $oldnode->port, 'regression' ]);
run_log([$ENV{PG_REGRESS}, '--schedule', './serial_schedule',
		'--dlpath', '.', '--bindir=', '--use-existing',
		'--port', $oldnode->port]);

# Take a dump before performing the upgrade as a base comparison.
run_log(['pg_dumpall', '--no-sync', '--port', $oldnode->port,
		 '-f', "$tempdir/dump1.sql"]);

# Move back to current directory, all logs generated need to be located
# at the origin.
chdir $startdir;

# Update the instance.
$oldnode->stop;

# pg_upgrade needs the location of the old and new binaries. This test
# relying on binaries being in PATH, so is pg_config. So fetch from it
# the real binary location.
my ($bindir, $stderr);
my $result = IPC::Run::run [ 'pg_config', '--bindir' ], '>', \$bindir, '2>', \$stderr;
chomp($bindir);

# Initialize a new node for the upgrade.
my $newnode = get_new_node('new_node');
$newnode->init;

# Time for the real run.
run_log(['pg_upgrade', '-d', $oldnode->data_dir, -D, $newnode->data_dir,
	'-b', $bindir, '-B', $bindir, '-p', $oldnode->port, '-P', $newnode->port]);
$newnode->start;

# Take a second dump on the upgraded instance.
run_log(['pg_dumpall', '--no-sync', '--port', $newnode->port,
		 '-f', "$tempdir/dump2.sql"]);

# Compare the two dumps, there should be no differences.
command_ok(['diff', '-q', "$tempdir/dump1.sql", "$tempdir/dump2.sql"],
		   'Old and new dump checks after pg_upgrade');
