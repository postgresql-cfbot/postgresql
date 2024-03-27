
# Copyright (c) 2023, PostgreSQL Global Development Group

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Execute a psql command and check its exit code and output on stdout and stderr.
sub psql_like
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	my ($node, $sql, $expected_exitcode, $expected_stdout, $expected_stderr, $test_name) = @_;

	my ($ret, $stdout, $stderr) = $node->psql('postgres', $sql);

	is($ret, $expected_exitcode, "$test_name: exit code $expected_exitcode");
	like($stdout, $expected_stdout, "$test_name: stdout matches");
	like($stderr, $expected_stderr, "$test_name: stderr matches");

	return;
}

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;

# prepare test table (with a non-standard primary key layout)
psql_like($node, "create table foo (t text, a int, b int, primary key (b, a))",
	0, qr/^$/, qr/^$/, 'create table');
psql_like($node, "insert into foo values ('foo', 1, 2), ('bar', 3, 4);",
	0, qr/^$/, qr/^$/, 'insert data');

# normal cases
$ENV{EDITOR} = 'true';
psql_like($node, 'select * from foo \gedit',
	0, qr/^$/, qr/\\gedit: no changes/, 'gedit without saving');

$ENV{EDITOR} = 'touch';
psql_like($node, 'select * from foo \gedit',
	0, qr/^$/, qr/\\gedit: no changes/, 'gedit without change');

$ENV{EDITOR} = 'sed -i -e s/bar/moo/';
psql_like($node, 'select * from foo \gedit',
	0,
	qr/UPDATE foo SET t = 'moo' WHERE b = '4' AND a = '3';/,
	qr/^$/, 'gedit with UPDATE');

$ENV{EDITOR} = "sed -i -e '1a{\"a\":6,\"b\":7},'";
psql_like($node, "select * from foo;\n\\gedit",
	0,
	qr/foo\|1\|2\nmoo\|3\|4\nINSERT INTO foo \(a, b\) VALUES \('6', '7'\);/,
	qr/^$/, 'gedit as separate command, with INSERT');

$ENV{EDITOR} = 'sed -i -e s/1/5/';
psql_like($node, 'select * from foo \gedit',
	0,
	qr/DELETE FROM foo WHERE b = '2' AND a = '1';.*INSERT INTO foo \(t, a, b\) VALUES \('foo', '5', '2'\);/s,
	qr/^$/, 'gedit with INSERT and DELETE');

$ENV{EDITOR} = 'sed -i -e /moo/d';
psql_like($node, 'select * from foo \gedit',
	0,
	qr/DELETE FROM foo WHERE b = '4' AND a = '3';/,
	qr/^$/, 'gedit with DELETE');

$ENV{EDITOR} = 'sed -i -e s/foo/baz/';
psql_like($node, 'select * from foo \gedit (key=b)',
	0,
	qr/UPDATE foo SET t = 'baz' WHERE b = '2';/,
	qr/^$/, 'gedit with custom key');

# error cases
$ENV{EDITOR} = 'touch';
psql_like($node, "select blub \\gedit",
	3, qr/^$/, qr/\\gedit: could not determine table name from query/, 'no FROM in query');

psql_like($node, "blub from blub \\gedit",
	3, qr/^$/, qr/ERROR:  syntax error at or near "blub"/, 'syntax error reported normally');

psql_like($node, "select * from blub \\gedit",
	3, qr/^$/, qr/ERROR:  relation "blub" does not exist/, 'gedit on missing table');

psql_like($node, "select * from blub \\gedit (bla)",
	3, qr/^$/, qr/\\gedit: unknown option "bla"/, 'unknown gedit option');

psql_like($node, "select * from blub \\gedit (blub=moo)",
	3, qr/^$/, qr/\\gedit: unknown option "blub"/, 'unknown gedit option with value');

psql_like($node, 'select * from foo \gedit (table=bar)',
	3, qr/^$/, qr/\\gedit: table "bar" does not exist/, 'gedit with custom table name');

psql_like($node, 'select a from foo \gedit',
	3, qr/^$/, qr/\\gedit: no key of table "foo" is contained in the returned query columns/,
	'key missing in query');

psql_like($node, 'select a from foo \gedit (key=a,b,c)',
	3, qr/^$/, qr/\\gedit: key column "b" not found in query/,
	'custom key missing in query');

$node->stop;

done_testing();
