use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 16;

# Test set-up
my ($node, $port);
$node = get_new_node('test');
$node->init;
$node->start;
$port = $node->port;

# Load the amcheck extension, upon which pg_amcheck depends
$node->safe_psql('postgres', q(CREATE EXTENSION amcheck));

#########################################
# Test connecting to a non-existent database

command_fails_like(
	[ 'pg_amcheck', '-p', $port, 'qqq' ],
	qr/database "qqq" does not exist/,
	'checking a non-existent database');

#########################################
# Test connecting with a non-existent user

command_fails_like(
	[ 'pg_amcheck', '-p', $port, '-U=no_such_user', 'postgres' ],
	qr/role "=no_such_user" does not exist/,
	'checking with a non-existent user');

#########################################
# Test checking a database without amcheck installed, by name.  We should see a
# message about missing amcheck

$node->command_like(
	[ 'pg_amcheck', '-p', $port, 'template1' ],
	qr/pg_amcheck: skipping database "template1": amcheck is not installed/,
	'checking a database by name without amcheck installed');

#########################################
# Test checking a database without amcheck installed, by only indirectly using
# a dbname pattern.  In verbose mode, we should see a message about missing
# amcheck

$node->command_like(
	[ 'pg_amcheck', '-p', $port, '-v', '-d', '*', 'postgres' ],
	qr/pg_amcheck: skipping database "template1": amcheck is not installed/,
	'checking a database by dbname implication without amcheck installed');

#########################################
# Test checking non-existent schemas, tables, and indexes

$node->command_ok(
	[ 'pg_amcheck', '-p', $port, '-s', 'no_such_schema' ],
	'checking a non-existent schema');

$node->command_ok(
	[ 'pg_amcheck', '-p', $port, '-t', 'no_such_table' ],
	'checking a non-existent table');

$node->command_ok(
	[ 'pg_amcheck', '-p', $port, '-i', 'no_such_index' ],
	'checking a non-existent schema');

$node->command_ok(
	[ 'pg_amcheck', '-p', $port, '-s', 'no*such*schema*' ],
	'no matching schemas');

$node->command_ok(
	[ 'pg_amcheck', '-p', $port, '-t', 'no*such*table*' ],
	'no matching tables');

$node->command_ok(
	[ 'pg_amcheck', '-p', $port, '-i', 'no*such*index' ],
	'no matching indexes');
