# Copyright (c) 2022, PostgreSQL Global Development Group

use strict;
use warnings;
use Cwd;
use Config;
use File::Basename qw(basename dirname);
use File::Path qw(rmtree);
use Fcntl qw(:seek);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

program_help_ok('pg_subscriber');
program_version_ok('pg_subscriber');
program_options_handling_ok('pg_subscriber');

my $tempdir = PostgreSQL::Test::Utils::tempdir;

my $node = PostgreSQL::Test::Cluster->new('publisher');
$node->init(allows_streaming => 'logical');
$node->start;

$node->command_fails_like(
	['pg_subscriber'],
	qr/no subscriber data directory specified/,
	'target directory must be specified');
$node->command_fails_like(
	['pg_subscriber', '-D', $tempdir],
	qr/no publisher connection string specified/,
	'publisher connection string must be specified');
$node->command_fails_like(
	['pg_subscriber', '-D', $tempdir, '-P', 'dbname=postgres'],
	qr/no subscriber connection string specified/,
	'subscriber connection string must be specified');
$node->command_fails_like(
	['pg_subscriber', '-D', $tempdir, '-P', 'dbname=postgres', '-S', 'dbname=postgres'],
	qr/is not a database cluster directory/,
	'directory must be a real database cluster directory');

done_testing();
