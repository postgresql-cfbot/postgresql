
# Copyright (c) 2026, PostgreSQL Global Development Group

# CREATE DATABASE file_copy racing checksum enable: the strategy check runs
# before the transaction has an XID, so the launcher can miss both.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use FindBin;
use lib $FindBin::RealBin;

use DataChecksums::Utils;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('filecopy_node');
$node->init(no_data_checksums => 1);
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');
$node->safe_psql('postgres',
	"CREATE TABLE t AS SELECT generate_series(1,10000) AS a;");

$node->safe_psql('postgres',
	"SELECT injection_points_attach('createdb-before-catalog-insert','wait');"
);
$node->safe_psql('postgres',
	"SELECT injection_points_attach('datachecksumsworker-fake-temptable-wait','wait');"
);

# Hold CREATE DATABASE after the strategy check, before its xact is visible.
my $bg = $node->background_psql('postgres');
$bg->query_until(
	qr/starting_create/, q(
\echo starting_create
CREATE DATABASE fcdb TEMPLATE template0 STRATEGY file_copy;
));
$node->wait_for_event('client backend', 'createdb-before-catalog-insert');

# Enable checksums, worker holds before processing template0.
enable_data_checksums($node);
$node->wait_for_event('datachecksums worker',
	'datachecksumsworker-fake-temptable-wait');

# Release CREATE DATABASE, must fail on the recheck instead of raw-copying.
$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('createdb-before-catalog-insert');");
$node->safe_psql('postgres',
	"SELECT injection_points_detach('createdb-before-catalog-insert');");

# Wait for the CREATE DATABASE xact to finish before releasing the worker.
$node->poll_query_until('postgres',
	    "SELECT count(*) = 0 FROM pg_stat_activity "
	  . "WHERE query LIKE 'CREATE DATABASE%' AND state != 'idle';");

$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('datachecksumsworker-fake-temptable-wait');"
);
$node->safe_psql('postgres',
	"SELECT injection_points_detach('datachecksumsworker-fake-temptable-wait');"
);

wait_for_checksum_state($node, 'on');

my $result = $node->safe_psql('postgres',
	"SELECT count(*) FROM pg_database WHERE datname = 'fcdb';");
is($result, '0', 'file_copy database creation was refused');

$bg->quit;

$node->stop;
command_ok([ 'pg_checksums', '--check', '-D', $node->data_dir ],
	'offline checksum verification passes');

done_testing();
