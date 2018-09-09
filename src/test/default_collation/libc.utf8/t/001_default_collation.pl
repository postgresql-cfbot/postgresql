use strict;
use warnings;

use Config;
use PostgresNode;
use TestLib;
use Test::More tests => 168;

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

sub psql
{
	my ($command, $db) = @_;
	my ($result, $in, $out, $err);
	my @psql = ('psql', '-X', '-c', $command);
	if (defined($db))
	{
		push(@psql, $db);
	}
	print "# Running: " . join(" ", @psql) . "\n";
	$result = IPC::Run::run \@psql, \$in, \$out, \$err;
	($result, $out, $err);
}

# --locale

test_initdb(
	"be_BY\@latin libc locale",
	"--locale=be_BY\@latin\@libc",
	"");

test_initdb(
	"be_BY\@latin libc locale invalid modifier order",
	"--locale=be_BY\@libc\@latin",
	"invalid locale name \"be_BY\@libc\@latin\"");

# --lc-collate

test_initdb(
	"be_BY\@latin libc lc_collate",
	"--lc-collate=be_BY\@latin\@libc",
	"");

test_initdb(
	"be_BY\@latin libc lc_collate invalid modifier order",
	"--lc-collate=be_BY\@libc\@latin",
	"invalid locale name \"be_BY\@libc\@latin\" \\(provider \"libc\"\\)");

# test createdb, CREATE DATABASE and default collation behaviour

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

		($result, $out_command, $err_command) = psql(
			"select datcollate from pg_database where datname = 'mydb';");

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

	($result, $out_command, $err_command) = psql(
				"create database mydb "
			  . $options
			  . ($from_template0 ? " TEMPLATE = template0;" : ";"));

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

		($result, $out_command, $err_command) = psql(
			"select datcollate from pg_database where datname = 'mydb';");

		like($out_command,
			 qr{\@libc\n},
			 "CREATE DATABASE: $test_name: check pg_database.datcollate");

		@command = ("dropdb mydb");
		print "# Running: " . join(" ", @command) . "\n";
		system(@command);
	}
}

sub test_default_collation
{
	my ($createdb_options, $collation, @commands) = @_;
	my (@command, $result, $in_command, $out_command, $err_command);

	@command = ("createdb", split(" ", $createdb_options), "mydb");
	print "# Running: " . join(" ", @command) . "\n";
	$result = IPC::Run::run \@command, \$in_command, \$out_command,
		\$err_command;

	ok($result, "\"@command\" exit code 0");
	is($err_command, "", "\"@command\" no stderr");

	($result, $out_command, $err_command) = psql(
		"select datcollate from pg_database where datname = 'mydb';");

	like($out_command, qr{\@libc\n}, "\"@command\" check output");

	for (my $row = 0; $row <= $#commands; $row++)
	{
		my ($command_text, $expected) = @{$commands[$row]};
		($result, $out_command, $err_command) = psql($command_text, "mydb");

		ok($result, "\"@command\" exit code 0");
		is($err_command, "", "\"@command\" no stderr");
		if ($out_command)
		{
			is(
				$out_command,
				$expected,
				"default collation "
			  . $collation
			  . ": \""
			  . $command_text
			  . "\" check output");
		}
	}

	@command = ("dropdb mydb");
		print "# Running: " . join(" ", @command) . "\n";
		system(@command);
}

my $node = get_new_node('main');
$node->init;
$node->start;
local $ENV{PGPORT} = $node->port;

# test createdb

# --locale

test_createdb(
	"be_BY\@latin libc locale",
	"--locale=be_BY\@latin\@libc",
	1,
	"");

test_createdb(
	"be_BY\@latin libc locale invalid modifier order",
	"--locale=be_BY\@libc\@latin",
	1,
	"invalid locale name: \"be_BY\@libc\@latin\"");

# --lc-collate

test_createdb(
	"be_BY\@latin libc lc_collate",
	"--lc-collate=be_BY\@latin\@libc",
	1,
	"");

test_createdb(
	"be_BY\@latin libc lc_collate invalid modifier order",
	"--lc-collate=be_BY\@libc\@latin",
	1,
	"invalid locale name: \"be_BY\@libc\@latin\" \\(provider \"libc\"\\)");

# test CREATE DATABASE

# LC_COLLATE

test_create_database(
	"be_BY\@latin libc lc_collate",
	"LC_COLLATE = 'be_BY\@latin\@libc'",
	1,
	"");

test_create_database(
	"be_BY\@latin libc lc_collate invalid modifier order",
	"LC_COLLATE = 'be_BY\@libc\@latin'",
	1,
	"invalid locale name: \"be_BY\@libc\@latin\" \\(provider \"libc\"\\)");

# test default collation behaviour
# use commands and outputs from the regression test collate.linux.utf8

test_default_collation(
	"--lc-collate=en_US.utf8\@libc --template=template0",
	"en_US.utf8\@libc",
	(
		[
			"CREATE TABLE collate_test1 (a int, b text NOT NULL);",
			"CREATE TABLE\n"
		],
		[
			"INSERT INTO collate_test1 VALUES "
		  . "(1, 'abc'), (2, 'äbc'), (3, 'bbc'), (4, 'ABC');",
			"INSERT 0 4\n"],
		[
			"SELECT * FROM collate_test1 WHERE b >= 'bbc';",
			" a |  b  \n"
		  . "---+-----\n"
		  . " 3 | bbc\n"
		  . "(1 row)\n"
		  . "\n"
		],
		[
			"SELECT a, b FROM collate_test1 ORDER BY b;",
			" a |  b  \n"
		  . "---+-----\n"
		  . " 1 | abc\n"
		  . " 4 | ABC\n"
		  . " 2 | äbc\n"
		  . " 3 | bbc\n"
		  . "(4 rows)\n"
		  . "\n"
		],
		# star expansion
		[
			"SELECT * FROM collate_test1 ORDER BY b;",
			" a |  b  \n"
		  . "---+-----\n"
		  . " 1 | abc\n"
		  . " 4 | ABC\n"
		  . " 2 | äbc\n"
		  . " 3 | bbc\n"
		  . "(4 rows)\n"
		  . "\n"
		],
		# upper/lower
		["CREATE TABLE collate_test10 (a int, x text);", "CREATE TABLE\n", ""],
		[
			"INSERT INTO collate_test10 VALUES (1, 'hij'), (2, 'HIJ');",
			"INSERT 0 2\n"
		],
		[
			"SELECT a, lower(x), upper(x), initcap(x) FROM collate_test10;",
			" a | lower | upper | initcap \n"
		  . "---+-------+-------+---------\n"
		  . " 1 | hij   | HIJ   | Hij\n"
		  . " 2 | hij   | HIJ   | Hij\n"
		  . "(2 rows)\n"
		  . "\n"
		],
		# LIKE/ILIKE
		[
			"SELECT * FROM collate_test1 WHERE b LIKE 'abc';",
			" a |  b  \n"
		  . "---+-----\n"
		  . " 1 | abc\n"
		  . "(1 row)\n"
		  . "\n"
		],
		[
			"SELECT * FROM collate_test1 WHERE b LIKE 'abc%';",
			" a |  b  \n"
		  . "---+-----\n"
		  . " 1 | abc\n"
		  . "(1 row)\n"
		  . "\n"
		],
		[
			"SELECT * FROM collate_test1 WHERE b LIKE '%bc%';",
			" a |  b  \n"
		  . "---+-----\n"
		  . " 1 | abc\n"
		  . " 2 | äbc\n"
		  . " 3 | bbc\n"
		  . "(3 rows)\n"
		  . "\n"
		],
		[
			"SELECT * FROM collate_test1 WHERE b ILIKE 'abc';",
			" a |  b  \n"
		  . "---+-----\n"
		  . " 1 | abc\n"
		  . " 4 | ABC\n"
		  . "(2 rows)\n"
		  . "\n"
		],
		[
			"SELECT * FROM collate_test1 WHERE b ILIKE 'abc%';",
			" a |  b  \n"
		  . "---+-----\n"
		  . " 1 | abc\n"
		  . " 4 | ABC\n"
		  . "(2 rows)\n"
		  . "\n"
		],
		[
			"SELECT * FROM collate_test1 WHERE b ILIKE '%bc%';",
			" a |  b  \n"
		  . "---+-----\n"
		  . " 1 | abc\n"
		  . " 2 | äbc\n"
		  . " 3 | bbc\n"
		  . " 4 | ABC\n"
		  . "(4 rows)\n"
		  . "\n"
		],
		[
			"SELECT 'Türkiye' ILIKE '%KI%' AS \"true\";",
			" true \n"
		  . "------\n"
		  . " t\n"
		  . "(1 row)\n"
		  . "\n"
		],
		[
			"SELECT 'bıt' ILIKE 'BIT' AS \"false\";",
			" false \n"
		  . "-------\n"
		  . " f\n"
		  . "(1 row)\n"
		  . "\n"
		],
		# regular expressions
		[
			"SELECT * FROM collate_test1 WHERE b ~ '^abc\$';",
			" a |  b  \n"
		  . "---+-----\n"
		  . " 1 | abc\n"
		  . "(1 row)\n"
		  . "\n"
		],
		[
			"SELECT * FROM collate_test1 WHERE b ~ '^abc';",
			" a |  b  \n"
		  . "---+-----\n"
		  . " 1 | abc\n"
		  . "(1 row)\n"
		  . "\n"
		],
		[
			"SELECT * FROM collate_test1 WHERE b ~ 'bc';",
			" a |  b  \n"
		  . "---+-----\n"
		  . " 1 | abc\n"
		  . " 2 | äbc\n"
		  . " 3 | bbc\n"
		  . "(3 rows)\n"
		  . "\n"
		],
		[
			"SELECT * FROM collate_test1 WHERE b ~* '^abc\$';",
			" a |  b  \n"
		  . "---+-----\n"
		  . " 1 | abc\n"
		  . " 4 | ABC\n"
		  . "(2 rows)\n"
		  . "\n"
		],
		[
			"SELECT * FROM collate_test1 WHERE b ~* '^abc';",
			" a |  b  \n"
		  . "---+-----\n"
		  . " 1 | abc\n"
		  . " 4 | ABC\n"
		  . "(2 rows)\n"
		  . "\n"
		],
		[
			"SELECT * FROM collate_test1 WHERE b ~* 'bc';",
			" a |  b  \n"
		  . "---+-----\n"
		  . " 1 | abc\n"
		  . " 2 | äbc\n"
		  . " 3 | bbc\n"
		  . " 4 | ABC\n"
		  . "(4 rows)\n"
		  . "\n"
		],
		["CREATE TABLE collate_test6 (a int, b text);", "CREATE TABLE\n", ""],
		[
			"INSERT INTO collate_test6 VALUES "
		  . "(1, 'abc'), (2, 'ABC'), (3, '123'), (4, 'ab1'), "
		  . "(5, 'a1!'), (6, 'a c'), (7, '!.;'), (8, '   '), "
		  . "(9, 'äbç'), (10, 'ÄBÇ');",
			"INSERT 0 10\n"
		],
		[
			"SELECT b, "
		  . "b ~ '^[[:alpha:]]+\$' AS is_alpha, "
		  . "b ~ '^[[:upper:]]+\$' AS is_upper, "
		  . "b ~ '^[[:lower:]]+\$' AS is_lower, "
		  . "b ~ '^[[:digit:]]+\$' AS is_digit, "
		  . "b ~ '^[[:alnum:]]+\$' AS is_alnum, "
		  . "b ~ '^[[:graph:]]+\$' AS is_graph, "
		  . "b ~ '^[[:print:]]+\$' AS is_print, "
		  . "b ~ '^[[:punct:]]+\$' AS is_punct, "
		  . "b ~ '^[[:space:]]+\$' AS is_space "
		  . "FROM collate_test6;",
			"  b  | is_alpha | is_upper | is_lower | is_digit | is_alnum | is_graph | is_print | is_punct | is_space \n"
		  . "-----+----------+----------+----------+----------+----------+----------+----------+----------+----------\n"
		  . " abc | t        | f        | t        | f        | t        | t        | t        | f        | f\n"
		  . " ABC | t        | t        | f        | f        | t        | t        | t        | f        | f\n"
		  . " 123 | f        | f        | f        | t        | t        | t        | t        | f        | f\n"
		  . " ab1 | f        | f        | f        | f        | t        | t        | t        | f        | f\n"
		  . " a1! | f        | f        | f        | f        | f        | t        | t        | f        | f\n"
		  . " a c | f        | f        | f        | f        | f        | f        | t        | f        | f\n"
		  . " !.; | f        | f        | f        | f        | f        | t        | t        | t        | f\n"
		  . "     | f        | f        | f        | f        | f        | f        | t        | f        | t\n"
		  . " äbç | t        | f        | t        | f        | t        | t        | t        | f        | f\n"
		  . " ÄBÇ | t        | t        | f        | f        | t        | t        | t        | f        | f\n"
		  . "(10 rows)\n"
		  . "\n"
		],
		[
			"SELECT 'Türkiye' ~* 'KI' AS \"true\";",
			" true \n"
		  . "------\n"
		  . " t\n"
		  . "(1 row)\n"
		  . "\n"
		],
		[
			"SELECT 'bıt' ~* 'BIT' AS \"false\";",
			" false \n"
		  . "-------\n"
		  . " f\n"
		  . "(1 row)\n"
		  . "\n"
		],
		[
			"SELECT a, lower(coalesce(x, 'foo')) FROM collate_test10;",
			" a | lower \n"
		  . "---+-------\n"
		  . " 1 | hij\n"
		  . " 2 | hij\n"
		  . "(2 rows)\n"
		  . "\n"
		],
		[
			"SELECT a, b, greatest(b, 'CCC') FROM collate_test1 ORDER BY 3;",
			" a |  b  | greatest \n"
		  . "---+-----+----------\n"
		  . " 1 | abc | CCC\n"
		  . " 2 | äbc | CCC\n"
		  . " 3 | bbc | CCC\n"
		  . " 4 | ABC | CCC\n"
		  . "(4 rows)\n"
		  . "\n"
		],
		[
			"SELECT a, x, lower(greatest(x, 'foo')) FROM collate_test10;",
			" a |  x  | lower \n"
		  . "---+-----+-------\n"
		  . " 1 | hij | hij\n"
		  . " 2 | HIJ | hij\n"
		  . "(2 rows)\n"
		  . "\n"
		],
		[
			"SELECT a, nullif(b, 'abc') FROM collate_test1 ORDER BY 2;",
			" a | nullif \n"
		  . "---+--------\n"
		  . " 4 | ABC\n"
		  . " 2 | äbc\n"
		  . " 3 | bbc\n"
		  . " 1 | \n"
		  . "(4 rows)\n"
		  . "\n"
		],
		[
			"SELECT a, lower(nullif(x, 'foo')) FROM collate_test10;",
			" a | lower \n"
		  . "---+-------\n"
		  . " 1 | hij\n"
		  . " 2 | hij\n"
		  . "(2 rows)\n"
		  . "\n"
		],
		[
			"SELECT a, CASE b WHEN 'abc' THEN 'abcd' ELSE b END "
		  . "FROM collate_test1 ORDER BY 2;",
			" a |  b   \n"
		  . "---+------\n"
		  . " 4 | ABC\n"
		  . " 2 | äbc\n"
		  . " 1 | abcd\n"
		  . " 3 | bbc\n"
		  . "(4 rows)\n"
		  . "\n"
		],
		["CREATE DOMAIN testdomain AS text;", "CREATE DOMAIN\n", ""],
		[
			"SELECT a, b::testdomain FROM collate_test1 ORDER BY 2;",
			" a |  b  \n"
		  . "---+-----\n"
		  . " 1 | abc\n"
		  . " 4 | ABC\n"
		  . " 2 | äbc\n"
		  . " 3 | bbc\n"
		  . "(4 rows)\n"
		  . "\n"
		],
		[
			"SELECT a, lower(x::testdomain) FROM collate_test10;",
			" a | lower \n"
		  . "---+-------\n"
		  . " 1 | hij\n"
		  . " 2 | hij\n"
		  . "(2 rows)\n"
		  . "\n"
		],
		[
			"SELECT min(b), max(b) FROM collate_test1;",
			" min | max \n"
		  . "-----+-----\n"
		  . " abc | bbc\n"
		  . "(1 row)\n"
		  . "\n",
			""
		],
		[
			"SELECT array_agg(b ORDER BY b) FROM collate_test1;",
			"     array_agg     \n"
		  . "-------------------\n"
		  . " {abc,ABC,äbc,bbc}\n"
		  . "(1 row)\n"
		  . "\n"
		],
		[
			"SELECT a, b FROM collate_test1 "
		  . "UNION ALL "
		  . "SELECT a, b FROM collate_test1 ORDER BY 2;",
			" a |  b  \n"
		  . "---+-----\n"
		  . " 1 | abc\n"
		  . " 1 | abc\n"
		  . " 4 | ABC\n"
		  . " 4 | ABC\n"
		  . " 2 | äbc\n"
		  . " 2 | äbc\n"
		  . " 3 | bbc\n"
		  . " 3 | bbc\n"
		  . "(8 rows)\n"
		  . "\n"
		],
		# casting
		[
			"SELECT a, CAST(b AS varchar) FROM collate_test1 ORDER BY 2;",
			" a |  b  \n"
		  . "---+-----\n"
		  . " 1 | abc\n"
		  . " 4 | ABC\n"
		  . " 2 | äbc\n"
		  . " 3 | bbc\n"
		  . "(4 rows)\n"
		  . "\n"
		],
		# propagation of collation in SQL functions (inlined and non-inlined
		# cases) and plpgsql functions too
		[
			"CREATE FUNCTION mylt (text, text) RETURNS boolean LANGUAGE sql "
		  . "AS \$\$ select \$1 < \$2 \$\$;",
			"CREATE FUNCTION\n"
		],
		[
			"CREATE FUNCTION mylt_noninline (text, text) "
		  . "RETURNS boolean LANGUAGE sql "
		  . "AS \$\$ select \$1 < \$2 limit 1 \$\$;",
			"CREATE FUNCTION\n"
		],
		[
			"CREATE FUNCTION mylt_plpgsql (text, text) "
		  . "RETURNS boolean LANGUAGE plpgsql "
		  . "AS \$\$ begin return \$1 < \$2; end \$\$;",
			"CREATE FUNCTION\n"
		],
		[
			"SELECT a.b AS a, b.b AS b, a.b < b.b AS lt, "
		  . "mylt(a.b, b.b), mylt_noninline(a.b, b.b), mylt_plpgsql(a.b, b.b) "
		  . "FROM collate_test1 a, collate_test1 b "
		  . "ORDER BY a.b, b.b;",
			"  a  |  b  | lt | mylt | mylt_noninline | mylt_plpgsql \n"
		  . "-----+-----+----+------+----------------+--------------\n"
		  . " abc | abc | f  | f    | f              | f\n"
		  . " abc | ABC | t  | t    | t              | t\n"
		  . " abc | äbc | t  | t    | t              | t\n"
		  . " abc | bbc | t  | t    | t              | t\n"
		  . " ABC | abc | f  | f    | f              | f\n"
		  . " ABC | ABC | f  | f    | f              | f\n"
		  . " ABC | äbc | t  | t    | t              | t\n"
		  . " ABC | bbc | t  | t    | t              | t\n"
		  . " äbc | abc | f  | f    | f              | f\n"
		  . " äbc | ABC | f  | f    | f              | f\n"
		  . " äbc | äbc | f  | f    | f              | f\n"
		  . " äbc | bbc | t  | t    | t              | t\n"
		  . " bbc | abc | f  | f    | f              | f\n"
		  . " bbc | ABC | f  | f    | f              | f\n"
		  . " bbc | äbc | f  | f    | f              | f\n"
		  . " bbc | bbc | f  | f    | f              | f\n"
		  . "(16 rows)\n"
		  . "\n"
		],
		# polymorphism
		[
			"SELECT * FROM unnest("
		  . "(SELECT array_agg(b ORDER BY b) FROM collate_test1)"
		  . ") ORDER BY 1;",
			" unnest \n"
		  . "--------\n"
		  . " abc\n"
		  . " ABC\n"
		  . " äbc\n"
		  . " bbc\n"
		  . "(4 rows)\n"
		  . "\n"
		],
		[
			"CREATE FUNCTION dup (anyelement) RETURNS anyelement "
		  . "AS 'select \$1' LANGUAGE sql;",
			"CREATE FUNCTION\n"
		],
		[
			"SELECT a, dup(b) FROM collate_test1 ORDER BY 2;",
			" a | dup \n"
		  . "---+-----\n"
		  . " 1 | abc\n"
		  . " 4 | ABC\n"
		  . " 2 | äbc\n"
		  . " 3 | bbc\n"
		  . "(4 rows)\n"
		  . "\n"
		],
		# indexes
		[
			"CREATE INDEX collate_test1_idx1 ON collate_test1 (b);",
			"CREATE INDEX\n"
		]
	)
);

$node->stop;
