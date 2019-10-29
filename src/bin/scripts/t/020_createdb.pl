use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 16;

program_help_ok('createdb');
program_version_ok('createdb');
program_options_handling_ok('createdb');

my $node = get_new_node('main');
$node->init;
$node->start;

$node->issues_sql_like(
	[ 'createdb', 'foobar1' ],
	qr/statement: CREATE DATABASE foobar1/,
	'SQL CREATE DATABASE run');
$node->issues_sql_like(
	[ 'createdb', '-l', 'C', '-E', 'LATIN1', '-T', 'template0', 'foobar2' ],
	qr/statement: CREATE DATABASE foobar2 ENCODING 'LATIN1'/,
	'create database with encoding');

if ($ENV{with_icu} eq 'yes')
{
	$node->issues_sql_like(
		[ 'createdb', '-T', 'template0', '--collation-provider=icu', 'foobar3' ],
		qr/statement: CREATE DATABASE foobar3 .* COLLATION_PROVIDER icu/,
		'create database with ICU');
}
else
{
	$node->command_fails(
		[ 'createdb', '-T', 'template0', '--collation-provider=icu', 'foobar3' ],
		'create database with ICU fails since no ICU support');
	pass;
}

$node->command_fails([ 'createdb', 'foobar1' ],
	'fails if database already exists');
$node->command_fails([ 'createdb', '-T', 'template0', '--collation-provider=xyz', 'foobarX' ],
	'fails for invalid collation provider');
