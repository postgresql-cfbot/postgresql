use strict;
use warnings;

use Config;
use PostgresNode;
use TestLib;
use Test::More;

if ($ENV{with_icu} eq 'yes')
{
	plan tests => 12;
}
else
{
	plan skip_all => 'ICU not supported by this build';
}

#### Set up the server

note "setting up data directory";
my $node = get_new_node('main');
$node->init;

$ENV{PGHOST} = $node->host;
$ENV{PGPORT} = $node->port;
$node->start;

sub test_index
{
	my ($err_like, $err_comm) = @_;

	my ($ret, $out, $err) = $node->psql('postgres',
		'SET enable_seqscan = 0;'
		. "EXPLAIN SELECT val FROM icu1 WHERE val = '0'");

	is($ret, 0, 'EXPLAIN should succeed.');
	like($out, qr/icu1_fr/, 'Index icu1_fr should be used.');
	like($err, $err_like, $err_comm);
}

$node->safe_psql('postgres',
	'CREATE TABLE icu1(val text);'
	. 'INSERT INTO icu1 SELECT i::text FROM generate_series(1, 10000) i;'
	. 'CREATE INDEX icu1_fr ON icu1 (val COLLATE "fr-x-icu");');
$node->safe_psql('postgres', 'VACUUM ANALYZE icu1;');

test_index(qr/^$/, 'No warning should be raised');

# Simulate different collation version
$node->safe_psql('postgres',
	"UPDATE pg_depend SET refobjversion = 'not_a_version'"
	. " WHERE refobjversion IS NOT NULL"
	. " AND objid::regclass::text = 'icu1_fr';");

test_index(qr/index "icu1_fr" depends on collation "fr-x-icu" version "not_a_version", but the current version is/,
	'Different collation version warning should be raised.');

# Simulate unknown collation version
$node->safe_psql('postgres',
	"UPDATE pg_depend SET refobjversion = ''"
	. " WHERE refobjversion IS NOT NULL"
	. " AND objid::regclass::text = 'icu1_fr';");

test_index(qr/index "icu1_fr" depends on collation "fr-x-icu" with an unknown version, and the current version is/,
	'Unknown collation version warning should be raised.');

# Simulate previously unhandled collation versioning
$node->safe_psql('postgres',
	"UPDATE pg_depend SET refobjversion = NULL"
	. " WHERE refobjversion IS NOT NULL"
	. " AND objid::regclass::text = 'icu1_fr';");

test_index(qr/index "icu1_fr" depends on collation "fr-x-icu" with an unknown version, and the current version is/,
	'Unknown collation version warning should be raised.');

$node->stop;
