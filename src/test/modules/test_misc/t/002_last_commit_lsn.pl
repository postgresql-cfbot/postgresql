
# Copyright (c) 2021-2022, PostgreSQL Global Development Group

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 2;

# Initialize a test cluster
my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init();
$node->start;

my ($ret, $before_lsn, $after_lsn);

$ret = $node->safe_psql('postgres', 'SELECT pg_last_commit_lsn()');
is($ret, '', 'null at startup');

$node->safe_psql('postgres', qq[CREATE TABLE t(i integer);]);

$before_lsn = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
$node->safe_psql('postgres', 'INSERT INTO t(i) VALUES (1)');
$after_lsn = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

$ret = $node->safe_psql('postgres', qq[
  SELECT pg_last_commit_lsn() BETWEEN '$before_lsn'::pg_lsn AND '$after_lsn'::pg_lsn
]);
is($ret, 't', 'last commit lsn is set');

$node->stop('fast');
