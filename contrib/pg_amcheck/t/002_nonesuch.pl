use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 14;

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
	[ 'pg_amcheck', '-p', "$port", 'qqq' ],
	qr/\Qpg_amcheck: error: could not connect to server: FATAL:  database "qqq" does not exist\E/,
	'connecting to a non-existent database');

#########################################
# Test connecting with a non-existent user

command_fails_like(
	[ 'pg_amcheck', '-p', "$port", '-U=no_such_user' ],
	qr/\Qpg_amcheck: error: could not connect to server: FATAL:  role "=no_such_user" does not exist\E/,
	'connecting with a non-existent user');

#########################################
# Test checking a non-existent schema, table, and patterns with --strict-names

command_fails_like(
	[ 'pg_amcheck', '-p', "$port", '-n', 'nonexistent' ],
	qr/\Qpg_amcheck: error: no matching schemas were found\E/,
	'checking a non-existent schema');

command_fails_like(
	[ 'pg_amcheck', '-p', "$port", '-t', 'nonexistent' ],
	qr/\Qpg_amcheck: error: no matching tables were found\E/,
	'checking a non-existent table');

command_fails_like(
	[ 'pg_amcheck', '-p', "$port", '--strict-names', '-n', 'nonexistent*' ],
	qr/\Qpg_amcheck: error: no matching schemas were found for pattern\E/,
	'no matching schemas');

command_fails_like(
	[ 'pg_amcheck', '-p', "$port", '--strict-names', '-t', 'nonexistent*' ],
	qr/\Qpg_amcheck: error: no matching tables were found for pattern\E/,
	'no matching tables');

command_fails_like(
	[ 'pg_amcheck', '-p', "$port", '--strict-names', '-i', 'nonexistent*' ],
	qr/\Qpg_amcheck: error: no matching indexes were found for pattern\E/,
	'no matching indexes');
