use strict;
use warnings;

use Config;
use PostgresNode;
use TestLib;
use Test::More;

my $tempdir = TestLib::tempdir;
my $datadir = "$tempdir/data";

# check whether ICU can convert C locale to a language tag

my ($in_initdb, $out_initdb, $err_initdb);
my @command = (qw(initdb -A trust -N -D), $datadir, "--locale=C\@icu");
print "# Running: " . join(" ", @command) . "\n";
my $result = IPC::Run::run \@command, \$in_initdb, \$out_initdb, \$err_initdb;

my $c_to_icu_language_tag = (
	not $err_initdb =~ /ICU error: could not convert locale name "C" to language tag: U_ILLEGAL_ARGUMENT_ERROR/);

# get the number of tests

plan tests => $c_to_icu_language_tag ? 124 : 110;

# test initdb

sub test_initdb
{
	my ($test_name, $options, $expected_collprovider, $error_message) = @_;
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
			 qr{The default collation provider is \"$expected_collprovider\"\.},
			 "initdb: $test_name: check output");
	}

	File::Path::rmtree $datadir;
}

# --locale

test_initdb(
	"empty libc locale",
	"--locale=\@libc",
	"libc",
	"");

test_initdb(
	"C locale without collation provider",
	"--locale=C",
	"libc",
	"");

test_initdb(
	"POSIX locale without collation provider",
	"--locale=POSIX",
	"libc",
	"");

test_initdb(
	"C libc locale",
	"--locale=C\@libc",
	"libc",
	"");

test_initdb(
	"POSIX libc locale",
	"--locale=POSIX\@libc",
	"libc",
	"");

test_initdb(
	"C icu locale",
	"--locale=C\@icu",
	"",
	($c_to_icu_language_tag ?
	 "selected encoding \\(SQL_ASCII\\) is not supported for ICU locales" :
	 "ICU error: could not convert locale name \"C\" to language tag: U_ILLEGAL_ARGUMENT_ERROR"));

test_initdb(
	"POSIX icu locale",
	"--locale=POSIX\@icu",
	"",
	($c_to_icu_language_tag ?
	 "selected encoding \\(SQL_ASCII\\) is not supported for ICU locales" :
	 "ICU error: could not convert locale name \"C\" to language tag: U_ILLEGAL_ARGUMENT_ERROR"));

test_initdb(
	"C locale too many modifiers",
	"--locale=C\@icu\@libc",
	"",
	"invalid locale name \"C\@icu\"");

test_initdb(
	"ICU language tag format locale",
	"--locale=und-x-icu",
	"",
	"invalid locale name \"und-x-icu\"");

# --lc-collate with the same --lc-ctype if needed

test_initdb(
	"empty libc lc_collate",
	"--lc-collate=\@libc",
	"libc",
	"");

test_initdb(
	"C lc_collate without collation provider",
	"--lc-collate=C",
	"libc",
	"");

test_initdb(
	"POSIX lc_collate without collation provider",
	"--lc-collate=POSIX",
	"libc",
	"");

test_initdb(
	"C libc lc_collate",
	"--lc-collate=C\@libc",
	"libc",
	"");

test_initdb(
	"POSIX libc lc_collate",
	"--lc-collate=POSIX\@libc",
	"libc",
	"");

test_initdb(
	"C icu lc_collate",
	"--lc-collate=C\@icu --lc-ctype=C",
	"",
	($c_to_icu_language_tag ?
	 "selected encoding \\(SQL_ASCII\\) is not supported for ICU locales" :
	 "ICU error: could not convert locale name \"C\" to language tag: U_ILLEGAL_ARGUMENT_ERROR"));;

test_initdb(
	"POSIX icu lc_collate",
	"--lc-collate=POSIX\@icu --lc-ctype=POSIX",
	"",
	($c_to_icu_language_tag ?
	 "selected encoding \\(SQL_ASCII\\) is not supported for ICU locales" :
	 "ICU error: could not convert locale name \"C\" to language tag: U_ILLEGAL_ARGUMENT_ERROR"));;

test_initdb(
	"C lc_collate too many modifiers",
	"--lc-collate=C\@icu\@libc",
	"",
	"invalid locale name \"C\@icu\"");

test_initdb(
	"ICU language tag format lc_collate",
	"--lc-collate=und-x-icu",
	"",
	"invalid locale name \"und-x-icu\"");

# --locale & --lc-collate

test_initdb(
	"lc_collate implicit provider takes precedence",
	"--locale=\@icu --lc-collate=C",
	"libc",
	"");

test_initdb(
	"lc_collate explicit provider takes precedence",
	"--locale=C\@libc --lc-collate=C\@icu",
	"",
	($c_to_icu_language_tag ?
	 "selected encoding \\(SQL_ASCII\\) is not supported for ICU locales" :
	 "ICU error: could not convert locale name \"C\" to language tag: U_ILLEGAL_ARGUMENT_ERROR"));

# test createdb and CREATE DATABASE

sub test_createdb
{
	my ($test_name, $options, $expected_collprovider, $error_message) = @_;
	my (@command, $result, $in_command, $out_command, $err_command);

	@command = ("createdb",
				split(" ", $options),
				"--template=template0",
				"mydb");

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

		if ($expected_collprovider eq "libc")
		{
			like($out_command,
				 qr{\@$expected_collprovider\n},
				 "createdb: $test_name: check pg_database.datcollate");
		}
		elsif ($expected_collprovider eq "icu")
		{
			like($out_command,
				 qr{\@$expected_collprovider([\.\d]+)?\n},
				 "createdb: $test_name: check pg_database.datcollate");
		}

		@command = ("dropdb mydb");
		print "# Running: " . join(" ", @command) . "\n";
		system(@command);
	}
}

sub test_create_database
{
	my ($test_name,
		$createdb_options,
		$psql_options,
		$expected_collprovider,
		$error_message) = @_;
	my (@command, $result, $in_command, $out_command, $err_command);

	@command = ("psql",
				split(" ", $psql_options),
				"-c",
				"create database mydb "
			  . $createdb_options
			  . " template = template0;");
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

		if ($expected_collprovider eq "libc")
		{
			like($out_command,
				 qr{\@$expected_collprovider\n},
				 "CREATE DATABASE: $test_name: check pg_database.datcollate");
		}
		elsif ($expected_collprovider eq "icu")
		{
			like($out_command,
				 qr{\@$expected_collprovider([\.\d]+)?\n},
				 "CREATE DATABASE: $test_name: check pg_database.datcollate");
		}

		@command = ("dropdb mydb");
		print "# Running: " . join(" ", @command) . "\n";
		system(@command);
	}
}

my $node = get_new_node('main');
$node->init;
$node->start;
local $ENV{PGPORT} = $node->port;

@command = ("createuser --createdb --no-superuser non_superuser");
print "# Running: " . join(" ", @command) . "\n";
system(@command);

# test createdb

# --locale

test_createdb(
	"empty libc locale",
	"--locale=\@libc",
	"libc",
	"");

test_createdb(
	"C locale without collation provider",
	"--locale=C",
	"libc",
	"");

test_createdb(
	"POSIX locale without collation provider",
	"--locale=POSIX",
	"libc",
	"");

test_createdb(
	"C libc locale",
	"--locale=C\@libc",
	"libc",
	"");

test_createdb(
	"POSIX libc locale",
	"--locale=POSIX\@libc",
	"libc",
	"");

if ($c_to_icu_language_tag)
{
	test_createdb(
		"C icu locale with SQL_ASCII encoding and superuser",
		"--locale=C\@icu --encoding=SQL_ASCII",
		"icu",
		"");
}
else
{
	test_createdb(
		"C icu locale with SQL_ASCII encoding and superuser",
		"--locale=C\@icu --encoding=SQL_ASCII",
		"",
		"ICU error: could not convert locale name \"C\" to language tag: U_ILLEGAL_ARGUMENT_ERROR");
}

test_createdb(
	"C icu locale with SQL_ASCII encoding and non-superuser",
	"--locale=C\@icu --encoding=SQL_ASCII --username=non_superuser",
	"",
	"encoding \"SQL_ASCII\" is not supported for ICU locales");

if ($c_to_icu_language_tag)
{
	test_createdb(
		"POSIX icu locale with SQL_ASCII encoding and superuser",
		"--locale=POSIX\@icu --encoding=SQL_ASCII",
		"icu",
		"");
}
else
{
	test_createdb(
		"POSIX icu locale with SQL_ASCII encoding and superuser",
		"--locale=POSIX\@icu --encoding=SQL_ASCII",
		"",
		"ICU error: could not convert locale name \"C\" to language tag: U_ILLEGAL_ARGUMENT_ERROR");
}

test_createdb(
	"POSIX icu locale with SQL_ASCII encoding and non-superuser",
	"--locale=POSIX\@icu --encoding=SQL_ASCII --username=non_superuser",
	"",
	"encoding \"SQL_ASCII\" is not supported for ICU locales");

test_createdb(
	"C locale too many modifiers",
	"--locale=C\@icu\@libc",
	"",
	"invalid locale name: \"C\@icu\"");

test_createdb(
	"ICU language tag format locale",
	"--locale=und-x-icu",
	"",
	"invalid locale name: \"und-x-icu\"");

# --lc-collate with the same --lc-ctype if needed

test_createdb(
	"empty libc lc_collate",
	"--lc-collate=\@libc",
	"libc",
	"");

test_createdb(
	"C lc_collate without collation provider",
	"--lc-collate=C --lc-ctype=C",
	"libc",
	"");

test_createdb(
	"POSIX lc_collate without collation provider",
	"--lc-collate=POSIX --lc-ctype=POSIX",
	"libc",
	"");

test_createdb(
	"C libc lc_collate",
	"--lc-collate=C\@libc --lc-ctype=C",
	"libc",
	"");

test_createdb(
	"POSIX libc lc_collate",
	"--lc-collate=POSIX\@libc --lc-ctype=POSIX",
	"libc",
	"");

if ($c_to_icu_language_tag)
{
	test_createdb(
		"C icu lc_collate with SQL_ASCII encoding and superuser",
		"--lc-collate=C\@icu --lc-ctype=C --encoding=SQL_ASCII",
		"icu",
		"");
}
else
{
	test_createdb(
		"C icu lc_collate with SQL_ASCII encoding and superuser",
		"--lc-collate=C\@icu --lc-ctype=C --encoding=SQL_ASCII",
		"",
		"ICU error: could not convert locale name \"C\" to language tag: U_ILLEGAL_ARGUMENT_ERROR");
}

test_createdb(
	"C icu lc_collate with SQL_ASCII encoding and non-superuser",
	"--lc-collate=C\@icu --lc-ctype=C --encoding=SQL_ASCII "
  . "--username=non_superuser",
	"",
	"encoding \"SQL_ASCII\" is not supported for ICU locales");

if ($c_to_icu_language_tag)
{
	test_createdb(
		"POSIX icu lc_collate with SQL_ASCII encoding and superuser",
		"--lc-collate=POSIX\@icu --lc-ctype=POSIX --encoding=SQL_ASCII",
		"icu",
		"");

}
else
{
	test_createdb(
		"POSIX icu lc_collate with SQL_ASCII encoding and superuser",
		"--lc-collate=POSIX\@icu --lc-ctype=POSIX --encoding=SQL_ASCII",
		"",
		"ICU error: could not convert locale name \"C\" to language tag: U_ILLEGAL_ARGUMENT_ERROR");
}

test_createdb(
	"POSIX icu lc_collate with SQL_ASCII encoding and non-superuser",
	"--lc-collate=POSIX\@icu --lc-ctype=POSIX --encoding=SQL_ASCII "
  . "--username=non_superuser",
	"",
	"encoding \"SQL_ASCII\" is not supported for ICU locales");

test_createdb(
	"C lc_collate too many modifiers",
	"--lc-collate=C\@icu\@libc",
	"",
	"invalid locale name: \"C\@icu\"");

test_createdb(
	"ICU language tag format lc_collate",
	"--lc-collate=und-x-icu",
	"",
	"invalid locale name: \"und-x-icu\"");

# test CREATE DATABASE

# LC_COLLATE with the same LC_CTYPE if needed

test_create_database(
	"empty libc lc_collate",
	"LC_COLLATE = '\@libc'",
	"",
	"libc",
	"");

test_create_database(
	"C lc_collate without collation provider",
	"LC_COLLATE = 'C' LC_CTYPE = 'C'",
	"",
	"libc",
	"");

test_create_database(
	"POSIX lc_collate without collation provider",
	"LC_COLLATE = 'POSIX' LC_CTYPE = 'POSIX'",
	"",
	"libc",
	"");

test_create_database(
	"C libc lc_collate",
	"LC_COLLATE = 'C\@libc' LC_CTYPE = 'C'",
	"",
	"libc",
	"");

test_create_database(
	"POSIX libc lc_collate",
	"LC_COLLATE = 'POSIX\@libc' LC_CTYPE = 'POSIX'",
	"",
	"libc",
	"");

if ($c_to_icu_language_tag)
{
	test_create_database(
		"C icu lc_collate with SQL_ASCII encoding and superuser",
		"LC_COLLATE = 'C\@icu' LC_CTYPE = 'C' ENCODING = 'SQL_ASCII'",
		"",
		"icu",
		"");
}
else
{
	test_create_database(
		"C icu lc_collate with SQL_ASCII encoding and superuser",
		"LC_COLLATE = 'C\@icu' LC_CTYPE = 'C' ENCODING = 'SQL_ASCII'",
		"",
		"",
		"ICU error: could not convert locale name \"C\" to language tag: U_ILLEGAL_ARGUMENT_ERROR");
}

test_create_database(
	"C icu lc_collate with SQL_ASCII encoding and non-superuser",
	"LC_COLLATE = 'C\@icu' LC_CTYPE = 'C' ENCODING = 'SQL_ASCII'",
	"--username=non_superuser",
	"",
	"encoding \"SQL_ASCII\" is not supported for ICU locales");

if ($c_to_icu_language_tag)
{
	test_create_database(
		"POSIX icu lc_collate with SQL_ASCII encoding and superuser",
		"LC_COLLATE = 'POSIX\@icu' LC_CTYPE = 'POSIX' ENCODING = 'SQL_ASCII'",
		"",
		"icu",
		"");
}
else
{
	test_create_database(
		"POSIX icu lc_collate with SQL_ASCII encoding and superuser",
		"LC_COLLATE = 'POSIX\@icu' LC_CTYPE = 'POSIX' ENCODING = 'SQL_ASCII'",
		"",
		"",
		"ICU error: could not convert locale name \"C\" to language tag: U_ILLEGAL_ARGUMENT_ERROR");
}

test_create_database(
	"POSIX icu lc_collate with SQL_ASCII encoding and non-superuser",
	"LC_COLLATE = 'POSIX\@icu' LC_CTYPE = 'POSIX' ENCODING = 'SQL_ASCII'",
	"--username=non_superuser",
	"",
	"encoding \"SQL_ASCII\" is not supported for ICU locales");

test_create_database(
	"C lc_collate too many modifiers",
	"LC_COLLATE = 'C\@icu\@libc'",
	"",
	"",
	"invalid locale name: \"C\@icu\"");

test_create_database(
	"ICU language tag format lc_collate",
	"LC_COLLATE = 'und-x-icu'",
	"",
	"",
	"invalid locale name: \"und-x-icu\"");

$node->stop;
