use strict;
use warnings;

use Config;
use PostgresNode;
use TestLib;
use Test::More tests => 90;

my $tempdir = TestLib::tempdir;
my $datadir = "$tempdir/data";

# test initdb

sub test_initdb
{
	my ($test_name, $options, $error_message) = @_;
	my ($in_initdb, $out_initdb, $err_initdb);

	mkdir $datadir;

	my @command = (qw(initdb -A trust -N -D), $datadir, split(" ", $options));
	print "# Running: " . join(" ", @command) . "\n";
	my $result = IPC::Run::run \@command, \$in_initdb, \$out_initdb,
		\$err_initdb;

	if ($error_message)
	{
		like($err_initdb,
			 qr{$error_message},
			 "initdb: $test_name: check error message");
	}
	else
	{
		ok($result, "\"@command\" exit code 0");
		is($err_initdb, "", "\"@command\" no stderr");
		like($out_initdb,
			 qr{The default collation provider is \"libc\"\.},
			 "initdb: $test_name: check output");
	}

	File::Path::rmtree $datadir;
}

# empty locales

test_initdb(
	"empty locales",
	"",
	"");

# --locale

test_initdb(
	"empty libc locale",
	"--locale=\@libc",
	"");

test_initdb(
	"C locale without collation provider",
	"--locale=C",
	"");

test_initdb(
	"POSIX locale without collation provider",
	"--locale=POSIX",
	"");

test_initdb(
	"C libc locale",
	"--locale=C\@libc",
	"");

test_initdb(
	"C icu locale",
	"--locale=C\@icu",
	"ICU is not supported in this build");

test_initdb(
	"C locale too many modifiers",
	"--locale=C\@icu\@libc",
	"invalid locale name \"C\@icu\"");

# --lc-collate

test_initdb(
	"empty libc lc_collate",
	"--lc-collate=\@libc",
	"");

test_initdb(
	"C lc_collate without collation provider",
	"--lc-collate=C",
	"");

test_initdb(
	"POSIX lc_collate without collation provider",
	"--lc-collate=POSIX",
	"");

test_initdb(
	"C libc lc_collate",
	"--lc-collate=C\@libc",
	"");

test_initdb(
	"C icu lc_collate",
	"--lc-collate=C\@icu",
	"ICU is not supported in this build");

test_initdb(
	"C lc_collate too many modifiers",
	"--lc-collate=C\@icu\@libc",
	"invalid locale name \"C\@icu\" \\(provider \"libc\"\\)");

# --locale & --lc-collate

test_initdb(
	"lc_collate implicit provider takes precedence",
	"--locale=\@icu --lc-collate=C",
	"");

test_initdb(
	"lc_collate explicit provider takes precedence",
	"--locale=\@icu --lc-collate=\@libc",
	"");

# test createdb and CREATE DATABASE

sub test_createdb
{
	my ($test_name, $options, $from_template0, $error_message) = @_;
	my (@command, $result, $in_command, $out_command, $err_command);

	if ($from_template0)
	{
		$options = $options . " --template=template0";
	}

	@command = ("createdb", split(" ", $options), "mydb");
	print "# Running: " . join(" ", @command) . "\n";
	$result = IPC::Run::run \@command, \$in_command, \$out_command,
		\$err_command;

	if ($error_message)
	{
		like($err_command,
			 qr{$error_message},
			 "createdb: $test_name: check error message");
	}
	else
	{
		ok($result, "\"@command\" exit code 0");
		is($err_command, "", "\"@command\" no stderr");

		@command = (
			"psql",
			"-c",
			"select datcollate from pg_database where datname = 'mydb';");
		print "# Running: " . join(" ", @command) . "\n";
		$result = IPC::Run::run \@command, \$in_command, \$out_command,
			\$err_command;

		like($out_command,
			 qr{\@libc\n},
			 "createdb: $test_name: check pg_database.datcollate");

		@command = ("dropdb mydb");
		print "# Running: " . join(" ", @command) . "\n";
		system(@command);
	}
}

sub test_create_database
{
	my ($test_name, $options, $from_template0, $error_message) = @_;
	my (@command, $result, $in_command, $out_command, $err_command);

	@command = ("psql",
				"-c",
				"create database mydb "
			  . $options
			  . ($from_template0 ? " template = template0" : "")
			  . ";");
	print "# Running: " . join(" ", @command) . "\n";
	$result = IPC::Run::run \@command, \$in_command, \$out_command,
		\$err_command;

	if ($error_message)
	{
		like($err_command,
			 qr{$error_message},
			 "CREATE DATABASE: $test_name: check error message");
	}
	else
	{
		ok($result, "\"@command\" exit code 0");
		is($err_command, "", "\"@command\" no stderr");
		like($out_command, qr{CREATE DATABASE}, "\"@command\" check output");

		@command = (
			"psql",
			"-c",
			"select datcollate from pg_database where datname = 'mydb';");
		print "# Running: " . join(" ", @command) . "\n";
		$result = IPC::Run::run \@command, \$in_command, \$out_command,
			\$err_command;

		like($out_command,
			 qr{\@libc\n},
			 "CREATE DATABASE: $test_name: check pg_database.datcollate");

		@command = ("dropdb mydb");
		print "# Running: " . join(" ", @command) . "\n";
		system(@command);
	}
}

my $node = get_new_node('main');
$node->init;
$node->start;
local $ENV{PGPORT} = $node->port;

# test createdb

# empty locales

test_createdb(
	"empty locales",
	"",
	0,
	"");

# --locale

test_createdb(
	"empty libc locale",
	"--locale=\@libc",
	0,
	"");

test_createdb(
	"C locale without collation provider",
	"--locale=C",
	1,
	"");

test_createdb(
	"POSIX locale without collation provider",
	"--locale=POSIX",
	1,
	"");

test_createdb(
	"C libc locale",
	"--locale=C\@libc",
	1,
	"");

test_createdb(
	"C icu locale",
	"--locale=C\@icu",
	1,
	"ICU is not supported in this build");

test_createdb(
	"C locale too many modifiers",
	"--locale=C\@icu\@libc",
	1,
	"invalid locale name: \"C\@icu\"");

# --lc-collate

test_createdb(
	"empty libc lc_collate",
	"--lc-collate=\@libc",
	0,
	"");

test_createdb(
	"C lc_collate without collation provider",
	"--lc-collate=C",
	1,
	"");
test_createdb(
	"POSIX lc_collate without collation provider",
	"--lc-collate=POSIX",
	1,
	"");

test_createdb(
	"C libc lc_collate",
	"--lc-collate=C\@libc",
	1,
	"");

test_createdb(
	"C icu lc_collate",
	"--lc-collate=C\@icu",
	1,
	"ICU is not supported in this build");

test_createdb(
	"C lc_collate too many modifiers",
	"--lc-collate=C\@icu\@libc",
	1,
	"invalid locale name: \"C\@icu\" \\(provider \"libc\"\\)");

# test CREATE DATABASE

# empty locales

test_create_database(
	"empty locales",
	"",
	0,
	"");

# LC_COLLATE

test_create_database(
	"empty libc lc_collate",
	"LC_COLLATE = '\@libc'",
	0,
	"");

test_create_database(
	"C lc_collate without collation provider",
	"LC_COLLATE = 'C'",
	1,
	"");
test_create_database(
	"POSIX lc_collate without collation provider",
	"LC_COLLATE = 'POSIX'",
	1,
	"");

test_create_database(
	"C libc lc_collate",
	"LC_COLLATE = 'C\@libc'",
	1,
	"");

test_create_database(
	"C icu lc_collate",
	"LC_COLLATE = 'C\@icu'",
	1,
	"ICU is not supported in this build");

test_create_database(
	"C lc_collate too many modifiers",
	"LC_COLLATE = 'C\@icu\@libc'",
	1,
	"invalid locale name: \"C\@icu\" \\(provider \"libc\"\\)");

$node->stop;
