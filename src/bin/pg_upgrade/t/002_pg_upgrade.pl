# Set of tests for pg_upgrade.
use strict;
use warnings;

use Cwd qw(abs_path getcwd);
use File::Basename qw(dirname);

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
		next if $i == 7 || $i == 10 || $i == 13;    # skip BEL, LF, and CR
		$dbname = $dbname . sprintf('%c', $i);
	}
	$node->run_log([ 'createdb', '--port', $node->port, $dbname ]);
}

my $startdir = getcwd();

# From now on, the test of pg_upgrade consists in setting up an instance.
# This is the source instance used for the upgrade. Then a new and fresh
# instance is created, and is used as the target instance for the
# upgrade.  Before running an upgrade a logical dump of the old instance
# is taken, and a second logical dump of the new instance is taken after
# the upgrade.  The upgrade test passes if there are no differences after
# running pg_upgrade.

# Testing upgrades with an older instance of PostgreSQL requires
# setting up two environment variables, among the following:
# - "oldsrc", to point to the code source of the older version.
#   This is required to set up the old instance with pg_upgrade.
# - "olddump", to point to a dump file that will be used to set
#   up the old instance to upgrade from.
# - "oldinstall", to point to the installation path of the older
# version.

# "oldsrc" and "olddump" cannot be used together.  Setting up
# "olddump" and "oldinstall" will use the dump pointed to to
# set up the old instance.  If "oldsrc" is used instead of "olddump",
# the full set of regression tests of the old instance is run
# instead.

if (defined($ENV{oldsrc}) && defined($ENV{olddump}))
{
	die "oldsrc and olddump are both defined";
}
elsif (defined($ENV{oldsrc}))
{
	if (   (defined($ENV{oldsrc}) && !defined($ENV{oldinstall}))
		|| (!defined($ENV{oldsrc}) && defined($ENV{oldinstall})))
	{
		# Not all variables are defined, so leave and die if test is
		# done with an older installation.
		die "oldsrc or oldinstall is undefined";
	}
}
elsif (defined($ENV{olddump}))
{
	if (   (defined($ENV{olddump}) && !defined($ENV{oldinstall}))
		|| (!defined($ENV{olddump}) && defined($ENV{oldinstall})))
	{
		# Not all variables are defined, so leave and die if test is
		# done with an older installation.
		die "olddump or oldinstall is undefined";
	}
}

if ((defined($ENV{oldsrc}) || defined($ENV{olddump})) && $windows_os)
{
	# This configuration is not supported on Windows, as regress.so
	# location diverges across the compilation methods used on this
	# platform.
	die "No support for older version tests on Windows";
}

# Default is the location of this source code for both nodes used with
# the upgrade.
my $newsrc = abs_path("../../..");
my $oldsrc = $ENV{oldsrc} || $newsrc;
$oldsrc = abs_path($oldsrc);

# Temporary location for the dumps taken
my $tempdir = TestLib::tempdir;

# Initialize node to upgrade
my $oldnode = get_new_node('old_node', install_path => $ENV{oldinstall});

$oldnode->init(extra => [ '--locale', 'C', '--encoding', 'LATIN1' ]);
$oldnode->start;

# Set up the data of the old instance with pg_regress or an old dump.
if (defined($ENV{olddump}))
{
	# Use the dump specified.
	my $olddumpfile = $ENV{olddump};
	die "no dump file found!" unless -e $olddumpfile;

	# Load the dump, and we are done here.
	$oldnode->command_ok(
		[ 'psql', '-f', $olddumpfile, '--port', $oldnode->port, 'postgres' ]);
}
else
{
	# Default is to just use pg_regress to setup the old instance
	# Creating databases with names covering most ASCII bytes
	generate_db($oldnode, 1,  45);
	generate_db($oldnode, 46, 90);
	generate_db($oldnode, 91, 127);

	# Run core regression tests on the old instance.
	$oldnode->run_log([ "createdb", '--port', $oldnode->port, 'regression' ]);

	# This is more a trick than anything else, as pg_regress needs to be
	# from the old instance.  --dlpath is needed to be able to find the
	# location of regress.so, and it is located in the same folder as
	# pg_regress itself.

	# Grab any regression options that may be passed down by caller.
	my $extra_opts_val = $ENV{EXTRA_REGRESS_OPT} || "";
	my @extra_opts     = split(/\s+/, $extra_opts_val);

	chdir "$oldsrc/src/test/regress/";
	my @regress_command = [
		$ENV{PG_REGRESS},                  '--schedule',
		'parallel_schedule',               '--bindir',
		$oldnode->config_data('--bindir'), '--make-testtablespace-dir',
		'--dlpath',                        '.',
		'--use-existing',                  '--port',
		$oldnode->port
	];
	@regress_command = (@regress_command, @extra_opts);

	$oldnode->command_ok(@regress_command,
		'regression test run on old instance');

	# Move back to the start path.
	chdir $startdir;
}

# Before dumping, get rid of objects not existing or not supported in later
# versions. This depends on the version of the old server used, and matters
# only if different versions are used for the dump.
my ($result, $oldpgversion, $stderr) =
  $oldnode->psql('postgres', qq[SHOW server_version_num;]);
my $fix_sql;

if (defined($ENV{oldinstall}))
{
	# Changes for PostgreSQL ~13
	if ($oldpgversion < 140000)
	{
		# Postfix operators are not supported anymore in 14.
		$oldnode->psql(
			'regression', "
			DROP OPERATOR IF EXISTS #@# (bigint,NONE);
			DROP OPERATOR IF EXISTS #%# (bigint,NONE);
			DROP OPERATOR IF EXISTS !=- (bigint,NONE);
			DROP OPERATOR IF EXISTS #@%# (bigint,NONE);");
		# Last appeared in 13.
		$oldnode->psql('regression', "DROP FUNCTION public.putenv(text);");
	}

	# Changes for PostgreSQL ~9.6
	if ($oldpgversion < 100000)
	{
		# Last appeared in 9.6.
		$oldnode->psql('regression',
			"DROP FUNCTION public.oldstyle_length(integer, text);");
	}

	# Add here tweaks to objects to adapt to newer versions.
}

# Initialize a new node for the upgrade.  This is done early so as it is
# possible to know with which node's PATH the initial dump needs to be
# taken.
my $newnode = get_new_node('new_node');
$newnode->init(extra => [ '--locale=C', '--encoding=LATIN1' ]);
my $newbindir = $newnode->config_data('--bindir');
my $oldbindir = $oldnode->config_data('--bindir');

# Take a dump before performing the upgrade as a base comparison. Note
# that we need to use pg_dumpall from the new node here.
$newnode->command_ok(
	[
		'pg_dumpall', '--no-sync',
		'-d',         $oldnode->connstr('postgres'),
		'-f',         "$tempdir/dump1.sql"
	],
	'dump before running pg_upgrade');

# After dumping, update references to the old source tree's regress.so
# to point to the new tree.
if (defined($ENV{oldinstall}))
{
	# First, fetch all the references to libraries that are not part
	# of the default path $libdir.
	my $output = $oldnode->safe_psql('regression',
		"SELECT probin::text from pg_proc where probin not like '\$libdir%';"
	);
	chomp($output);
	my @libpaths = split("\n", $output);

	my $dump_data = slurp_file("$tempdir/dump1.sql");

	my $newregresssrc = "$newsrc/src/test/regress";
	foreach (@libpaths)
	{
		my $libpath = $_;
		$libpath = dirname($libpath);
		$dump_data =~ s/$libpath/$newregresssrc/g;
	}

	open my $fh, ">", "$tempdir/dump1.sql" or die "could not open dump file";
	print $fh $dump_data;
	close $fh;

	# This replaces any references to the old tree's regress.so
	# the new tree's regress.so.  Any references that do *not*
	# match $libdir are switched so as this request does not
	# depend on the path of the old source tree.  This is useful
	# when using an old dump.
	$oldnode->safe_psql(
		'regression', "UPDATE pg_proc SET probin =
	  regexp_replace(probin, '.*/', '$newregresssrc/')
	  WHERE probin NOT LIKE '\$libdir/%'");
}

# Move back to current directory, all logs generated need to be located
# at the origin.
chdir $startdir;

# Update the instance.
$oldnode->stop;

# Time for the real run.
chdir "$newsrc/src/test/regress";
$newnode->command_ok(
	[
		'pg_upgrade',       '-d', $oldnode->data_dir, '-D',
		$newnode->data_dir, '-b', $oldbindir,         '-B',
		$newbindir,         '-p', $oldnode->port,     '-P',
		$newnode->port
	],
	'run of pg_upgrade for new instance');
$newnode->start;

# Take a second dump on the upgraded instance.
$newnode->run_log(
	[
		'pg_dumpall', '--no-sync',
		'-d',         $newnode->connstr('postgres'),
		'-f',         "$tempdir/dump2.sql"
	]);

# Compare the two dumps, there should be no differences.
command_ok([ 'diff', '-q', "$tempdir/dump1.sql", "$tempdir/dump2.sql" ],
	'Old and new dump match after pg_upgrade');
