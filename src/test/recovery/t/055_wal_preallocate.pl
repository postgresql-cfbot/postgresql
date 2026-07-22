# Copyright (c) 2026, PostgreSQL Global Development Group

# Test pg_wal_preallocate(), which eagerly creates future WAL segments to
# cover a requested number of bytes (defaulting to min_wal_size).
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Count regular WAL segment files (24 hex digits) in a node's pg_wal.
sub count_segments
{
	my $node = shift;
	my $waldir = $node->data_dir . '/pg_wal';
	opendir(my $dh, $waldir) or die "could not open $waldir: $!";
	my @segs = grep { /^[0-9A-F]{24}$/ } readdir($dh);
	closedir($dh);
	return scalar @segs;
}

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init(
	allows_streaming => 1, extra => ['--wal-segsize=16']);
# Keep the automatic recycled pool small and stable so the effect of
# preallocation is clearly attributable to the function.
$node->append_conf(
	'postgresql.conf', q{
min_wal_size = 80MB
max_wal_size = 1GB
});
$node->start;

my $segsize = $node->safe_psql('postgres',
	"SELECT pg_size_bytes(current_setting('wal_segment_size'))");
note "wal_segment_size = $segsize bytes";

# Preallocate enough bytes for 10 segments; the pool should grow by the number
# reported, and (on a freshly started node) that number should be 10.
my $before = count_segments($node);
my $created =
  $node->safe_psql('postgres', "SELECT pg_wal_preallocate(10 * $segsize)");
is(count_segments($node), $before + $created,
	'pg_wal grew by the reported number of segments');
is($created, 10, 'pg_wal_preallocate(10 segments worth) reports 10 created');

# A second identical call creates nothing new.
is($node->safe_psql('postgres', "SELECT pg_wal_preallocate(10 * $segsize)"),
	0, 'second call creates no additional segments');

# Zero bytes is a no-op.
is($node->safe_psql('postgres', 'SELECT pg_wal_preallocate(0)'),
	0, 'zero bytes creates nothing');

# A negative byte count is rejected.
my ($ret, $stdout, $stderr) =
  $node->psql('postgres', 'SELECT pg_wal_preallocate(-1)');
isnt($ret, 0, 'negative byte count errors out');
like($stderr, qr/must not be negative/,
	'negative count produces the expected error');

# The no-argument form preallocates min_wal_size worth of segments.  Verify on
# a fresh node: the reported count matches the file growth, is positive, and an
# explicit request for exactly min_wal_size immediately afterwards is a no-op
# (which proves the default used min_wal_size).
my $node2 = PostgreSQL::Test::Cluster->new('defaults');
$node2->init(extra => ['--wal-segsize=16']);
$node2->append_conf(
	'postgresql.conf', q{
min_wal_size = 33MB
max_wal_size = 1GB
});
$node2->start;

my $segsize2 = $node2->safe_psql('postgres',
	"SELECT pg_size_bytes(current_setting('wal_segment_size'))");
my $min_wal_bytes = $node2->safe_psql('postgres',
	"SELECT pg_size_bytes(current_setting('min_wal_size'))");
my $default_nsegs = int(($min_wal_bytes + $segsize2 - 1) / $segsize2);
isnt($min_wal_bytes % $segsize2, 0,
	'test min_wal_size is not a whole number of segments');

my $before2 = count_segments($node2);
my $default_created = $node2->safe_psql('postgres', 'SELECT pg_wal_preallocate()');
is(count_segments($node2), $before2 + $default_created,
	'pg_wal grew by the reported number for the default call');
is($default_created, $default_nsegs,
	'no-argument call rounds min_wal_size up to whole segments');

is($node2->safe_psql('postgres', "SELECT pg_wal_preallocate($min_wal_bytes)"),
	0, 'explicit min_wal_size after default call is a no-op');

# An explicit NULL is treated like the default (the function is not strict), so
# it also finds the pool already warm here.
is($node2->safe_psql('postgres', 'SELECT pg_wal_preallocate(NULL)'),
	0, 'explicit NULL uses the default');
$node2->stop;

# Byte counts are rounded up, and preallocation is anchored at the exact WAL
# insertion position.  The restore point leaves that position within the first
# page after a segment switch; an approximate write pointer still identifies
# the previous segment there.
my $node3 = PostgreSQL::Test::Cluster->new('boundary');
$node3->init(
	allows_streaming => 1, extra => ['--wal-segsize=16']);
$node3->start;

my $segsize3 = $node3->safe_psql('postgres',
	"SELECT pg_size_bytes(current_setting('wal_segment_size'))");
my $before3 = count_segments($node3);
is($node3->safe_psql('postgres', 'SELECT pg_wal_preallocate(1)'),
	1, 'one byte rounds up to one segment');
is(count_segments($node3), $before3 + 1,
	'one-byte request creates one segment file');

$node3->safe_psql('postgres', q{
SELECT pg_switch_wal();
SELECT pg_create_restore_point('preallocation boundary test');
});
my $last_required = $node3->safe_psql(
	'postgres',
	"SELECT pg_walfile_name(pg_current_wal_insert_lsn() + 4 * $segsize3)");
is($node3->safe_psql('postgres', "SELECT pg_wal_preallocate(4 * $segsize3)"),
	4, 'preallocation starts after the exact insertion segment');
ok(-f $node3->data_dir . "/pg_wal/$last_required",
	'last requested future segment exists');
$node3->stop;

# The function is superuser-only by default, but EXECUTE can be granted.
$node->safe_psql('postgres', 'CREATE ROLE regress_wp_user LOGIN');
($ret, $stdout, $stderr) = $node->psql(
	'postgres',
	'SELECT pg_wal_preallocate(0)',
	extra_params => [ '-U', 'regress_wp_user' ]);
isnt($ret, 0, 'non-superuser is denied by default');
like($stderr, qr/permission denied/, 'permission denied for non-superuser');

$node->safe_psql('postgres',
	'GRANT EXECUTE ON FUNCTION pg_wal_preallocate(bigint) TO regress_wp_user');
($ret, $stdout, $stderr) = $node->psql(
	'postgres',
	'SELECT pg_wal_preallocate(0)',
	extra_params => [ '-U', 'regress_wp_user' ]);
is($ret, 0, 'granted non-superuser can execute');

# Recovery rejects the function even when the requested byte count is zero.
$node->backup('backup');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($node, 'backup', has_streaming => 1);
$standby->start;
($ret, $stdout, $stderr) =
  $standby->psql('postgres', 'SELECT pg_wal_preallocate(0)');
isnt($ret, 0, 'preallocation is rejected during recovery');
like($stderr, qr/recovery is in progress/,
	'recovery rejection produces the expected error');
$standby->stop;

$node->stop;

done_testing();
