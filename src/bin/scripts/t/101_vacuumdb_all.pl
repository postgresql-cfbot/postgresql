
# Copyright (c) 2021-2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;

$node->issues_sql_like(
	[ 'vacuumdb', '--all' ],
	qr/statement: VACUUM.*statement: VACUUM/s,
	'vacuum all databases');

$node->safe_psql(
	'postgres', q(
	CREATE DATABASE regression_invalid;
	UPDATE pg_database SET datconnlimit = -2 WHERE datname = 'regression_invalid';
));
$node->command_ok([ 'vacuumdb', '--all' ],
	'invalid database not targeted by vacuumdb -a');

# Doesn't quite belong here, but don't want to waste time by creating an
# invalid database in 010_vacuumdb.pl as well.
$node->command_fails_like(
	[ 'vacuumdb', '--dbname' => 'regression_invalid' ],
	qr/FATAL:  cannot connect to invalid database "regression_invalid"/,
	'vacuumdb cannot target invalid database');

# --exclude-database tests
$node->safe_psql('postgres', q(CREATE DATABASE regression_excl_test;));
$node->safe_psql('postgres', q(CREATE DATABASE regression_excl_test2;));

$node->command_checks_all(
	[ 'vacuumdb', '--all', '--exclude-database' => 'regression_excl_test' ],
	0,
	[qr/^(?!.*vacuuming database "regression_excl_test").*$/s],
	[qr//],
	'vacuumdb --all --exclude-database skips specified database');

$node->command_checks_all(
	[ 'vacuumdb', '--all', '-D' => 'regression_excl_test', '-D' => 'regression_excl_test2' ],
	0,
	[qr/^(?!.*vacuuming database "regression_excl_test")(?!.*vacuuming database "regression_excl_test2").*$/s],
	[qr//],
	'vacuumdb --all with multiple -D switches');

$node->command_fails_like(
	[ 'vacuumdb', '--exclude-database' => 'regression_excl_test' ],
	qr/cannot use the "exclude-database" option without the "all" option/,
	'cannot use --exclude-database without --all');

$node->command_fails_like(
	[ 'vacuumdb', '-d' => 'postgres', '--exclude-database' => 'regression_excl_test' ],
	qr/cannot use the "exclude-database" option with the "dbname" option/,
	'cannot use --exclude-database with --dbname');

done_testing();
