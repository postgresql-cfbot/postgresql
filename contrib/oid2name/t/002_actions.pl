
# Copyright (c) 2026, PostgreSQL Global Development Group

# Do basic object listsing supported by oid2name using
# an initialized cluster.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

sub get_oid
{
	my $node = shift;
	my $name = shift;
	return
	  int($node->safe_psql('postgres', "SELECT to_regclass('$name')::oid"));
}

my $node = PostgreSQL::Test::Cluster->new('mynode');
$node->init;
$node->start;

# Check default databases, tablespaces and system objects.
$node->command_like(['oid2name'], qr/postgres\s+pg_default\b/,
	"successfuly shows default databases with no arguments");

$node->command_like([ 'oid2name', '--tablespaces' ],
	qr/pg_default\b/, "successfuly shows default tablespaces");

$node->command_like([ 'oid2name', '--quiet', '--dbname' => 'postgres' ],
	qr/^$/, "succeeds in quiet mode");

my $pg_class_oid = get_oid($node, 'pg_class');
$node->command_like(
	[ 'oid2name', '--system-objects', '--dbname' => 'postgres' ],
	qr/${pg_class_oid}\s+pg_class\b/,
	"successfuly shows a system object");

# Create a test table with index and sequence, and then list the objects.
my $table = 't1';
my $index = "${table}_pkey";
my $seq = "${table}_id_seq";

$node->safe_psql('postgres', "CREATE TABLE $table(id SERIAL PRIMARY KEY)");

my $table_oid = get_oid($node, $table);
my $index_oid = get_oid($node, $index);
my $seq_oid = get_oid($node, $seq);

$node->command_like(
	[ 'oid2name', '--dbname' => 'postgres', '--table' => $table ],
	qr/${table_oid}\s+${table}\b/, "successfuly shows table $table");

$node->command_like([ 'oid2name', '--dbname' => 'postgres' ],
	qr/${table_oid}\s+${table}\b/,
	"successfuly shows table $table in the database output");

$node->command_like(
	[ 'oid2name', '--dbname' => 'postgres', '--oid' => $table_oid ],
	qr/${table_oid}\s+${table}\b/,
	"successfuly shows table $table for oid $table_oid");

$node->command_like(
	[ 'oid2name', '--dbname' => 'postgres', '--filenode' => $table_oid ],
	qr/${table_oid}\s+${table}\b/,
	"successfuly shows table $table for filenode $table_oid");

$node->command_like(
	[ 'oid2name', '--dbname' => 'postgres', '--indexes' ],
	qr/${index_oid}\s+${index}\b/,
	"successfuly shows index $index in the output with indexes");

$node->command_like([ 'oid2name', '--dbname' => 'postgres', '--indexes' ],
	qr/${seq_oid}\s+${seq}\b/,
	"successfuly shows sequence $seq in the output with indexes");

done_testing();
