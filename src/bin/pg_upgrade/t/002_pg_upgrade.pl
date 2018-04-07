# Set of tests for pg_upgrade.
use strict;
use warnings;
use Cwd;
use Config;
use File::Basename;
use File::Copy;
use IPC::Run;
use PostgresNode;
use TestLib;
use Test::More tests => 4;

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

# Determine the version set to use depending on what the caller wants.
# There are a couple of environment variables to scan for:
# 1) oldsrc, which points to the code tree of the old instance's binaries
#    which gets upgraded.
# 2) oldbindir, which points to the binaries of the old instance to
#    upgrade.
# 2) oldlibdir, which points to the libraries of the old instance to
#    upgrade.
# If the caller has not defined any of them, default values are used,
# pointing to the source tree where this script is located.
my $regress_bin = undef;
my $newsrc = "../../..";	# top of this code repository
my $newlibdir;
my $oldsrc = $newsrc;
my $oldlibdir = undef;
my $oldbindir = undef;
my $dlpath = undef;

my ($stdout, $stderr);
my $result = IPC::Run::run [ 'pg_config', '--libdir' ], '>',
	\$stdout, '2>', \$stderr
	or die "could not execute pg_config";
chomp($stdout);
$newlibdir = $stdout;

if (!defined($ENV{oldsrc}) &&
	!defined($ENV{oldlibdir}) &&
	!defined($ENV{oldbindir}))
{
	# No variables defined, so run the test using this version's
	# tree for both the new and old instances.
	$regress_bin = $ENV{PG_REGRESS};

	# Calculate this build's old library directory
	$oldlibdir = $newlibdir;

	# And this build's old binary directory
	$result = IPC::Run::run [ 'pg_config', '--bindir' ], '>',
		\$stdout, '2>', \$stderr
		or die "could not execute pg_config";
	chomp($stdout);
	$oldbindir = $stdout;

	# In this case download path is in src/test/regress.
	$dlpath = ".";
}
elsif (defined($ENV{oldsrc}) &&
	defined($ENV{oldbindir}) &&
	defined($ENV{oldlibdir}))
{
	# A run is wanted on an old version as base.
	$oldsrc = $ENV{oldsrc};
	$oldbindir = $ENV{oldbindir};
	$oldlibdir = $ENV{oldlibdir};
	# FIXME: this needs better tuning. Using "." or "$oldlibdir/postgresql"
	# causes the regression tests to pass but pg_upgrade to fail afterwards.
	$dlpath = "$oldlibdir";
	$regress_bin = "$oldsrc/src/test/regress/pg_regress";
}
else
{
	# Not all variables are defined, so leave and die.
	die "not all variables in oldsrc, oldlibdir and oldbindir are defined";
}

# Make sure the installed libraries come first in dynamic load paths.
$ENV{LD_LIBRARY_PATH}="$newlibdir/postgresql";
$ENV{DYLD_LIBRARY_PATH}="$newlibdir/postgresql";

# Temporary location for dumps taken
my $tempdir = TestLib::tempdir;

# Initialize node to upgrade
my $oldnode = get_new_node('old_node', $oldbindir);
$oldbindir = $oldnode->bin_dir;	# could be default value
$oldnode->init(extra => [ '--locale=C', '--encoding=LATIN1' ],
			   pg_regress => $regress_bin);
$oldnode->start;

# Creating databases with names covering most ASCII bytes
generate_db($oldnode, 1,  45);
generate_db($oldnode, 46, 90);
generate_db($oldnode, 91, 127);

# Install manually regress.so, this got forgotten in the process.
copy "$oldsrc/src/test/regress/regress.so",
	"$oldlibdir/postgresql/regress.so"
	unless (-e "$oldlibdir/regress.so");

# Run regression tests on the old instance, using the binaries of this
# instance. At the same time create a tablespace path needed for the
# tests, similarly to what "make check" creates.
chdir dirname($regress_bin);
rmdir "testtablespace";
mkdir "testtablespace";
$oldnode->run_log([ "$oldbindir/createdb", '--port', $oldnode->port,
					'regression' ]);

$oldnode->command_ok([$regress_bin, '--schedule', './serial_schedule',
		'--dlpath', "$dlpath", '--bindir', $oldnode->bin_dir,
		'--use-existing', '--port', $oldnode->port],
		'regression test run on old instance');

# Before dumping, get rid of objects not existing in later versions. This
# depends on the version of the old server used, and matters only if the
# old and new source paths
my $oldpgversion;
($result, $oldpgversion, $stderr) =
	$oldnode->psql('postgres', qq[SHOW server_version_num;]);
my $fix_sql;
if ($newsrc ne $oldsrc)
{
	if ($oldpgversion <= 80400)
	{
		$fix_sql = "DROP FUNCTION public.myfunc(integer); DROP FUNCTION public.oldstyle_length(integer, text);";
	}
	else
	{
		$fix_sql = "DROP FUNCTION public.oldstyle_length(integer, text);";
	}
	$oldnode->psql('postgres', $fix_sql);
}

# Take a dump before performing the upgrade as a base comparison. Note
# that we need to use pg_dumpall from PATH here.
$oldnode->command_ok(['pg_dumpall', '--no-sync', '--port', $oldnode->port,
					  '-f', "$tempdir/dump1.sql"],
					 'dump before running pg_upgrade');

# After dumping, update references to the old source tree's regress.so and
# such.
if ($newsrc ne $oldsrc)
{
	if ($oldpgversion <= 80400)
	{
		$fix_sql = "UPDATE pg_proc SET probin = replace(probin::text, '$oldsrc', '$newsrc')::bytea WHERE probin LIKE '$oldsrc%';";
	}
	else
	{
		$fix_sql = "UPDATE pg_proc SET probin = replace(probin, '$oldsrc', '$newsrc') WHERE probin LIKE '$oldsrc%';";
	}
	$oldnode->psql('postgres', $fix_sql);

	my $dump_data = slurp_file("$tempdir/dump1.sql");
	$dump_data =~ s/$oldsrc/$newsrc/g;

	open my $fh, ">", "$tempdir/dump1.sql" or die "could not open dump file";
	print $fh $dump_data;
	close $fh;
}

# Move back to current directory, all logs generated need to be located
# at the origin.
chdir $startdir;

# Update the instance.
$oldnode->stop;

# Initialize a new node for the upgrade.
my $newnode = get_new_node('new_node');
$newnode->init(extra => [ '--locale=C', '--encoding=LATIN1' ]);

# Time for the real run.
chdir "$newsrc/src/test/regress";
my $newbindir = $newnode->bin_dir;
command_ok(['pg_upgrade', '-d', $oldnode->data_dir, '-D', $newnode->data_dir,
	'-b', $oldnode->bin_dir, '-B', $newnode->bin_dir, '-p', $oldnode->port,
	'-P', $newnode->port], 'run of pg_upgrade for new instance');
$newnode->start;

# Take a second dump on the upgraded instance.
run_log(['pg_dumpall', '--no-sync', '--port', $newnode->port,
		 '-f', "$tempdir/dump2.sql"]);

# Compare the two dumps, there should be no differences.
command_ok(['diff', '-q', "$tempdir/dump1.sql", "$tempdir/dump2.sql"],
		   'Old and new dump checks after pg_upgrade');
