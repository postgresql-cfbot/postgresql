# Copyright (c) 2023, PostgreSQL Global Development Group

#
# Test checking options of pg_subscriber.
#

use strict;
use warnings;
use PostgreSQL::Test::Utils;
use Test::More;

program_help_ok('pg_subscriber');
program_version_ok('pg_subscriber');
program_options_handling_ok('pg_subscriber');

my $datadir = PostgreSQL::Test::Utils::tempdir;

command_fails(['pg_subscriber'],
	'no subscriber data directory specified');
command_fails(
	[
		'pg_subscriber',
		'--pgdata', $datadir
	],
	'no publisher connection string specified');
command_fails(
	[
		'pg_subscriber',
		'--pgdata', $datadir,
		'--publisher-conninfo', 'dbname=postgres'
	],
	'no subscriber connection string specified');
command_fails(
	[
		'pg_subscriber',
		'--pgdata', $datadir,
		'--publisher-conninfo', 'dbname=postgres',
		'--subscriber-conninfo', 'dbname=postgres'
	],
	'no database name specified');

done_testing();
