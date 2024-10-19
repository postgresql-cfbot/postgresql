
# Copyright (c) 2024, PostgreSQL Global Development Group

# Test issue with lost tuple in case of DirtySnapshot index scans
use strict;
use warnings;

use Config;
use Errno;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my ($node, $result);
$node = PostgreSQL::Test::Cluster->new('DirtyScan_test');
$node->init;
$node->append_conf('postgresql.conf', 'fsync = off');
$node->append_conf('postgresql.conf', 'autovacuum = off');
$node->start;
$node->safe_psql('postgres', q(CREATE EXTENSION injection_points));
$node->safe_psql('postgres', q(CREATE TABLE tbl(i int primary key, n int)));

$node->safe_psql('postgres', q(INSERT INTO tbl VALUES(42,1)));
$node->safe_psql('postgres', q(SELECT injection_points_attach('check_exclusion_or_unique_constraint_no_conflict', 'error')));

$node->pgbench(
	'--no-vacuum --client=40 --transactions=1000',
	0,
	[qr{actually processed}],
	[qr{^$}],
	'concurrent UPSERT',
	{
		'on_conflicts' => q(
			INSERT INTO tbl VALUES(42,1) on conflict(i) do update set n = EXCLUDED.n + 1;
		)
	});

$node->safe_psql('postgres', q(SELECT injection_points_detach('check_exclusion_or_unique_constraint_no_conflict')));

$node->stop;
done_testing();