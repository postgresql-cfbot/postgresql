# Copyright (c) 2023, PostgreSQL Global Development Group
use strict;
use warnings FATAL => 'all';
use Config;
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use Test::More;

my $withZlib = $ENV{with_zlib} eq 'yes';
my $withLz4 = $ENV{with_lz4} eq 'yes';
my $withZstd = $ENV{with_zstd} eq 'yes';
if (!$withZlib && !$withLz4 && !$withZstd)
{
	plan skip_all => 'no compression methods available';
}

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;
$node->append_conf('postgresql.conf', "libpq_compression = off");
$node->start;

# Use a string that any reasonable compression algorithm will compress
my $compressableString = "a" x 1000;

my $result;
$result = $node->safe_psql("postgres",
	"SELECT '$compressableString'; SELECT rx_socket_bytes < rx_pq_bytes, tx_socket_bytes < tx_pq_bytes FROM pg_stat_network_traffic;",
	connstr => $node->connstr . " compression=on");
is( (split "\n", $result)[-1], "f|f", 'successfully does not compress if server-side compression is disabled');

$node->append_conf('postgresql.conf', "libpq_compression = on");
$node->reload;

$result = $node->safe_psql("postgres",
	"SELECT '$compressableString'; SELECT rx_socket_bytes < rx_pq_bytes, tx_socket_bytes < tx_pq_bytes FROM pg_stat_network_traffic;",
	connstr => $node->connstr . " compression=on");
is( (split "\n", $result)[-1], "t|t", 'successfully compresses bidirectionally with default algorithms');

my $test_algorithm;
if ($withZlib)
{
	$test_algorithm = "gzip";
}
elsif($withLz4)
{
	$test_algorithm = "lz4";
}
elsif($withZstd)
{
	$test_algorithm = "zstd";
}
$result = $node->safe_psql("postgres",
	"SELECT '$compressableString'; SELECT rx_socket_bytes < rx_pq_bytes, tx_socket_bytes < tx_pq_bytes FROM pg_stat_network_traffic;",
	connstr => $node->connstr . " compression=$test_algorithm:compress=off");
is( (split "\n", $result)[-1], "f|t", 'successfully compresses unidirectionally with client compression disabled');

$node->append_conf('postgresql.conf', "libpq_compression = '$test_algorithm:compress=off'");
$node->reload;
$result = $node->safe_psql("postgres",
	"SELECT '$compressableString'; SELECT rx_socket_bytes < rx_pq_bytes, tx_socket_bytes < tx_pq_bytes FROM pg_stat_network_traffic;",
	connstr => "compression=on");
is( (split "\n", $result)[-1], "t|f", 'successfully compresses unidirectionally with server compression algorithm');

$node->append_conf('postgresql.conf', 'libpq_compression = on');
$node->reload;

SKIP: {
	skip "gzip not available", 1 unless $ENV{with_zlib};

	$result = $node->safe_psql("postgres",
		"SELECT '$compressableString'; SELECT rx_socket_bytes < rx_pq_bytes, tx_socket_bytes < tx_pq_bytes FROM pg_stat_network_traffic;",
		connstr => $node->connstr . " compression=gzip");
	is( (split "\n", $result)[-1], "t|t", 'successfully compresses bidirectionally with gzip');
}

SKIP: {
	skip "lz4 not available", 1 unless $ENV{with_lz4};

	$result = $node->safe_psql("postgres",
		"SELECT '$compressableString'; SELECT rx_socket_bytes < rx_pq_bytes, tx_socket_bytes < tx_pq_bytes FROM pg_stat_network_traffic;",
		connstr => $node->connstr . " compression=lz4");
	is( (split "\n", $result)[-1], "t|t", 'successfully compresses bidirectionally with lz4');
}

SKIP: {
	skip "zstd not available", 1 unless $ENV{with_zstd};

	$result = $node->safe_psql("postgres",
		"SELECT '$compressableString'; SELECT rx_socket_bytes < rx_pq_bytes, tx_socket_bytes < tx_pq_bytes FROM pg_stat_network_traffic;",
		connstr => $node->connstr . " compression=zstd");
	is( (split "\n", $result)[-1], "t|t", 'successfully compresses bidirectionally with zstd');
}

done_testing();
