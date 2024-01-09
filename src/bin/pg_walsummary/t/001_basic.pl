# Copyright (c) 2021-2023, PostgreSQL Global Development Group

use strict;
use warnings;
use PostgreSQL::Test::Utils;
use Test::More;

my $tempdir = PostgreSQL::Test::Utils::tempdir;

program_help_ok('pg_walsummary');
program_version_ok('pg_walsummary');
program_options_handling_ok('pg_walsummary');

command_fails_like(
	['pg_walsummary'],
	qr/no input files specified/,
	'input files must be specified');

done_testing();
