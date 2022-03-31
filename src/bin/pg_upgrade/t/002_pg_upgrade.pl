# Set of tests for pg_upgrade, including cross-version checks.
use strict;
use warnings;

use Cwd qw(abs_path getcwd);
use File::Basename qw(dirname);

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

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
	$node->run_log(
		[ 'createdb', '--host', $node->host, '--port', $node->port, $dbname ]
	);
}

# The test of pg_upgrade consists requires two clusters, an old one and
# and a new one, that gets upgraded.  Before running the upgrade, a logical
# dump of the old cluster is taken, and a second logical dump of the new one
# is taken after the upgrade.  The upgrade test passes if there are no
# differences in these two dumps.

# Testing upgrades with an older version of PostgreSQL requires setting up
# two environment variables, as of:
# - "olddump", to point to a dump file that will be used to set up the old
#   instance to upgrade from, the dump being restored in the old cluster.
# - "oldinstall", to point to the installation path of the old cluster.
if (   (defined($ENV{olddump}) && !defined($ENV{oldinstall}))
	|| (!defined($ENV{olddump}) && defined($ENV{oldinstall})))
{
	# Not all variables are defined, so leave and die if test is
	# done with an older installation.
	die "olddump or oldinstall is undefined";
}

# Temporary location for the dumps taken
my $tempdir = PostgreSQL::Test::Utils::tempdir;

# Initialize node to upgrade
my $oldnode = PostgreSQL::Test::Cluster->new('old_node',
	install_path => $ENV{oldinstall});

# To increase coverage of non-standard segment size and group access without
# increasing test runtime, run these tests with a custom setting.
# --allow-group-access and --wal-segsize have been added in v11.
$oldnode->init(extra => [ '--wal-segsize', '1', '--allow-group-access' ]);
$oldnode->start;

# The default location of the source code is the root of this directory.
my $srcdir = abs_path("../../..");

# Set up the data of the old instance with a dump or pg_regress.
if (defined($ENV{olddump}))
{
	# Use the dump specified.
	my $olddumpfile = $ENV{olddump};
	die "no dump file found!" unless -e $olddumpfile;

	# Load the dump, and we are done here.
	$oldnode->command_ok(
		[
			'psql',   '-X',           '-f',     $olddumpfile,
			'--port', $oldnode->port, '--host', $oldnode->host,
			'regression'
		]);
}
else
{
	# Default is to just use pg_regress to set up the old instance
	# Creating databases with names covering most ASCII bytes
	generate_db($oldnode, 1,  45);
	generate_db($oldnode, 46, 90);
	generate_db($oldnode, 91, 127);

	# Grab any regression options that may be passed down by caller.
	my $extra_opts_val = $ENV{EXTRA_REGRESS_OPT} || "";
	my @extra_opts     = split(/\s+/, $extra_opts_val);

	# --dlpath is needed to be able to find the location of regress.so and
	# any libraries the regression tests required.  This needs to point to
	# the old cluster when using it.  In the default case, fallback to
	# what the caller provided for REGRESS_SHLIB.
	my $dlpath = dirname($ENV{REGRESS_SHLIB});

	# --outputdir points to the path where to place the output files.
	my $outputdir = $PostgreSQL::Test::Utils::tmp_check;

	# --inputdir points to the path of the input files.
	my $inputdir = "$srcdir/src/test/regress";

	my @regress_command = [
		$ENV{PG_REGRESS},
		@extra_opts,
		'--dlpath',
		$dlpath,
		'--max-concurrent-tests',
		'20',
		'--bindir',
		$oldnode->config_data('--bindir'),
		'--host',
		$oldnode->host,
		'--port',
		$oldnode->port,
		'--schedule',
		"$srcdir/src/test/regress/parallel_schedule",
		'--outputdir',
		$outputdir,
		'--inputdir',
		$inputdir
	];

	$oldnode->command_ok(@regress_command,
		'regression test run on old instance');
}

# Before dumping, get rid of objects not existing or not supported in later
# versions. This depends on the version of the old server used, and matters
# only if different versions are used for the dump.
if (defined($ENV{oldinstall}))
{
	# Note that upgrade_adapt.sql from the new version is used, to
	# cope with an upgrade to this version.
	$oldnode->run_log(
		[
			'psql',   '-X',
			'-f',     "$srcdir/src/bin/pg_upgrade/upgrade_adapt.sql",
			'--port', $oldnode->port,
			'--host', $oldnode->host,
			'regression'
		]);
}

# Initialize a new node for the upgrade.  This is done early so as it is
# possible to know with which node's PATH the initial dump needs to be
# taken.
my $newnode = PostgreSQL::Test::Cluster->new('new_node');
$newnode->init(extra => [ '--wal-segsize', '1', '--allow-group-access' ]);
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
		"SELECT DISTINCT probin::text FROM pg_proc WHERE probin NOT LIKE '\$libdir%';"
	);
	chomp($output);
	my @libpaths = split("\n", $output);

	my $dump_data = slurp_file("$tempdir/dump1.sql");

	my $newregresssrc = "$srcdir/src/test/regress";
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
	# when using an old dump.  Do the operation on all the databases
	# that allow connections so as this includes the regression
	# database and anything the user has set up.
	$output = $oldnode->safe_psql('postgres',
		"SELECT datname FROM pg_database WHERE datallowconn;");
	chomp($output);
	my @datnames = split("\n", $output);
	foreach (@datnames)
	{
		my $datname = $_;
		$oldnode->safe_psql(
			$datname, "UPDATE pg_proc SET probin =
		  regexp_replace(probin, '.*/', '$newregresssrc/')
		  WHERE probin NOT LIKE '\$libdir/%'");
	}
}

# Upgrade the instance.
$oldnode->stop;
command_ok(
	[
		'pg_upgrade', '--no-sync',        '-d', $oldnode->data_dir,
		'-D',         $newnode->data_dir, '-b', $oldbindir,
		'-B',         $newbindir,         '-p', $oldnode->port,
		'-P',         $newnode->port
	],
	'run of pg_upgrade for new instance');
$newnode->start;

# Check if there are any logs coming from pg_upgrade, that would only be
# retained on failure.
my $log_path = $newnode->data_dir . "/pg_upgrade_output.d/log";
if (-d $log_path)
{
	foreach my $log (glob("$log_path/*"))
	{
		note "###########################";
		note "Contents of log file $log";
		note "###########################";
		my $log_contents = slurp_file($log);
		print "$log_contents\n";
	}
}

# Second dump from the upgraded instance.
$newnode->run_log(
	[
		'pg_dumpall', '--no-sync',
		'-d',         $newnode->connstr('postgres'),
		'-f',         "$tempdir/dump2.sql"
	]);

# Compare the two dumps, there should be no differences.
command_ok([ 'diff', '-q', "$tempdir/dump1.sql", "$tempdir/dump2.sql" ],
	'old and new dump match after pg_upgrade');

done_testing();
