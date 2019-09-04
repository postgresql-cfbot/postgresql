use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

use File::Copy;

use FindBin;
use lib $FindBin::RealBin;

if ($ENV{with_lz4} eq 'yes')
{
	plan tests => 10;
}
else
{
	plan skip_all => 'LZ4 not supported by this build';
}

#### Set up the server.
note "setting up data directory";
my $node = get_new_node('master');
$node->init;
$node->append_conf('postgresql.conf', qq[
compression_algorithm = lz4
wal_compression = on
]);
$node->start;

# Run this before we lock down access below.
my $result = $node->safe_psql('postgres', "SHOW compression_algorithm");
is($result, 'lz4', 'compression_algorithm set to lz4');

$node->safe_psql('postgres',
	qq[
	CREATE TABLE toast_test (
		id int,
		data text
	)]);

$node->safe_psql('postgres',
	'ALTER TABLE toast_test ALTER COLUMN data SET STORAGE MAIN');

# This will actually be compressed inline as it's easy to compress
$node->safe_psql('postgres',
qq[
	INSERT INTO toast_test
		SELECT n, repeat('toasted', 1000)
		  FROM generate_series(1, 100) s(n);
]);


my $toast_size = $node->safe_psql('postgres',
qq[
	SELECT pg_relation_size((SELECT reltoastrelid FROM pg_catalog.pg_class WHERE relname = 'toast_test'));
]);

ok($toast_size == 0, 'toast table is used');

$node->safe_psql('postgres',
	'ALTER TABLE toast_test ALTER COLUMN data SET STORAGE EXTENDED');

# Something less easily compressable so that it's in TOAST table
$node->safe_psql('postgres',
qq[
	INSERT INTO toast_test
		SELECT n, (SELECT string_agg(md5(t::text),'')
		             FROM generate_series(1, 200) q(t))
		  FROM generate_series(101, 200) s(n);
]);

$toast_size = $node->safe_psql('postgres',
qq[
	SELECT pg_relation_size((SELECT reltoastrelid FROM pg_catalog.pg_class WHERE relname = 'toast_test'));
]);

ok($toast_size > 0, 'toast table is used');

# check if we can select data
is($node->safe_psql('postgres',
		qq[SELECT id, length(data) FROM toast_test WHERE id = 1]),
	'1|7000', 'can select compressed data');
is($node->safe_psql('postgres',
		qq[SELECT id, length(data) FROM toast_test WHERE id = 200]),
	'200|6400', 'can select TOAST compressed data');

# test slicing
is($node->safe_psql('postgres',
		qq[SELECT id, substr(data, 1, 10) FROM toast_test WHERE id = 50]),
	'50|toastedtoa', 'slicing of compressed data works');

is($node->safe_psql('postgres',
		qq[SELECT id, substr(data, 1, 10) FROM toast_test WHERE id = 150]),
	'150|c4ca4238a0', 'slicing of TOAST works');

$node->append_conf('postgresql.conf', qq[
compression_algorithm = pglz
]);
$node->reload;

# Run this before we lock down access below.
$result = $node->safe_psql('postgres', "SHOW compression_algorithm");
is($result, 'pglz', 'compression_algorithm set to pglz');

$node->safe_psql('postgres',
qq[
	INSERT INTO toast_test
		SELECT n, (SELECT string_agg(md5(t::text),'')
		             FROM generate_series(1, 200) q(t))
		  FROM generate_series(201, 300) s(n);
]);

is($node->safe_psql('postgres',
		qq[SELECT id, length(data) FROM toast_test WHERE id IN (200, 201)]),
q[200|6400
201|6400], 'can select TOAST with different compression for different rows');

is($node->safe_psql('postgres',
		qq[SELECT id, substr(data, 1, 10) FROM toast_test WHERE id IN (150, 250)]),
q[150|c4ca4238a0
250|c4ca4238a0], 'slicing of TOAST works with different compression for different row');

done_testing();
