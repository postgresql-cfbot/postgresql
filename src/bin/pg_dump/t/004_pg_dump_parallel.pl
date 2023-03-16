
# Copyright (c) 2021-2023, PostgreSQL Global Development Group

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $dbname1 = 'regression_src';
my $dbname2 = 'regression_dest1';
my $dbname3 = 'regression_dest2';

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;

my $backupdir = $node->backup_dir;

$node->run_log([ 'createdb', $dbname1 ]);
$node->run_log([ 'createdb', $dbname2 ]);
$node->run_log([ 'createdb', $dbname3 ]);

$node->safe_psql(
	$dbname1,
	qq{
create type digit as enum ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9');
create table t0 (en digit, data int) partition by hash(en);
create table t0_p1 partition of t0 for values with (modulus 3, remainder 0);
create table t0_p2 partition of t0 for values with (modulus 3, remainder 1);
create table t0_p3 partition of t0 for values with (modulus 3, remainder 2);
insert into t0 select (x%10)::text::digit, x from generate_series(1,1000) x;
	});

$node->command_ok(
	[
		'pg_dump', '-Fd', '--no-sync', '-j2', '-f', "$backupdir/dump1",
		$node->connstr($dbname1)
	],
	'parallel dump');

$node->command_ok(
	[
		'pg_restore', '-v',
		'-d',         $node->connstr($dbname2),
		'-j3',        "$backupdir/dump1"
	],
	'parallel restore');

$node->command_ok(
	[
		'pg_dump', '-Fd', '--no-sync', '-j2', '-f', "$backupdir/dump2",
		'--load-via-partition-root', $node->connstr($dbname1)
	],
	'parallel dump via partition root');

$node->command_ok(
	[
		'pg_restore', '-v',
		'-d',         $node->connstr($dbname3),
		'-j3',        "$backupdir/dump2"
	],
	'parallel restore via partition root');

done_testing();
