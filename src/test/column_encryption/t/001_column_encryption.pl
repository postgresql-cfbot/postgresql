# Copyright (c) 2021-2022, PostgreSQL Global Development Group

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;


sub create_cmk
{
	my ($cmkname) = @_;
	my $cmkfilename = "${PostgreSQL::Test::Utils::tmp_check}/${cmkname}.pem";
	system_or_bail 'openssl', 'genpkey', '-algorithm', 'rsa', '-out', $cmkfilename;
	$node->safe_psql('postgres', qq{CREATE COLUMN MASTER KEY ${cmkname} WITH (realm = '')});
	return $cmkfilename;
}

sub create_cek
{
	my ($cekname, $bytes, $cmkname, $cmkfilename) = @_;

	# generate random bytes
	system_or_bail 'openssl', 'rand', '-out', "${PostgreSQL::Test::Utils::tmp_check}/${cekname}.bin", $bytes;

	# encrypt CEK using CMK
	system_or_bail 'openssl', 'pkeyutl', '-encrypt',
	  '-inkey', $cmkfilename,
	  '-pkeyopt', 'rsa_padding_mode:oaep',
	  '-in', "${PostgreSQL::Test::Utils::tmp_check}/${cekname}.bin",
	  '-out', "${PostgreSQL::Test::Utils::tmp_check}/${cekname}.bin.enc";

	my $cekenchex = unpack('H*', slurp_file "${PostgreSQL::Test::Utils::tmp_check}/${cekname}.bin.enc");

	# create CEK in database
	$node->safe_psql('postgres', qq{CREATE COLUMN ENCRYPTION KEY ${cekname} WITH VALUES (column_master_key = ${cmkname}, encrypted_value = '\\x${cekenchex}');});

	return;
}


my $cmk1filename = create_cmk('cmk1');
my $cmk2filename = create_cmk('cmk2');
create_cek('cek1', 32, 'cmk1', $cmk1filename);
create_cek('cek2', 48, 'cmk2', $cmk2filename);

$ENV{'PGCOLUMNENCRYPTION'} = '1';
$ENV{'PGCMKLOOKUP'} = '*=file:' . ${PostgreSQL::Test::Utils::tmp_check} . '/%k.pem';


$node->safe_psql('postgres', qq{
CREATE TABLE tbl1 (
    a int,
    b text ENCRYPTED WITH (column_encryption_key = cek1),
    c smallint ENCRYPTED WITH (column_encryption_key = cek1)
);
});

$node->safe_psql('postgres', qq{
CREATE TABLE tbl2 (
    a int,
    b text ENCRYPTED WITH (encryption_type = deterministic, column_encryption_key = cek1)
);
});

$node->safe_psql('postgres', qq{
CREATE TABLE tbl3 (
    a int,
    b text ENCRYPTED WITH (column_encryption_key = cek1),
    c text ENCRYPTED WITH (column_encryption_key = cek2, algorithm = 'AEAD_AES_192_CBC_HMAC_SHA_384')
);
});

$node->safe_psql('postgres', q{
INSERT INTO tbl1 (a, b, c) VALUES (1, $1, $2) \gencr 'val1' 11
INSERT INTO tbl1 (a, b, c) VALUES (2, $1, $2) \gencr 'val2' 22
});

# Expected ciphertext length is 2 blocks of AES output (2 * 16) plus
# half SHA-256 output (16) in hex encoding: (2 * 16 + 16) * 2 = 96
like($node->safe_psql('postgres', q{COPY (SELECT * FROM tbl1) TO STDOUT}),
	qr/1\t\\\\x[0-9a-f]{96}\t\\\\x[0-9a-f]{96}\n2\t\\\\x[0-9a-f]{96}\t\\\\x[0-9a-f]{96}/,
	'inserted data is encrypted');

my $result;

$result = $node->safe_psql('postgres', q{SELECT a, b, c FROM tbl1 \gdesc});
is($result,
	q(a|integer
b|text
c|smallint),
	'query result description has original type');

$result = $node->safe_psql('postgres', q{SELECT a, b, c FROM tbl1});
is($result,
	q(1|val1|11
2|val2|22),
	'decrypted query result');

{
	local $ENV{'PGCMKLOOKUP'} = '*=run:broken %k "%b"';
	$result = $node->psql('postgres', q{SELECT a, b, c FROM tbl1});
	isnt($result, 0, 'query fails with broken cmklookup run setting');
}

{
	local $ENV{'PGCMKLOOKUP'} = "*=run:perl ./test_run_decrypt.pl '${PostgreSQL::Test::Utils::tmp_check}' %k %a '%b'";

	$result = $node->safe_psql('postgres', q{SELECT a, b, c FROM tbl1});
	is($result,
		q(1|val1|11
2|val2|22),
		'decrypted query result with cmklookup run');
}

$node->safe_psql('postgres', q{
INSERT INTO tbl3 (a, b, c) VALUES (1, $1, $2) \gencr 'valB1' 'valC1'
});

$result = $node->safe_psql('postgres', q{SELECT a, b, c FROM tbl3});
is($result,
	q(1|valB1|valC1),
	'decrypted query result multiple keys');


$node->command_fails_like(['test_client', 'test1'], qr/not encrypted/, 'test client fails because parameters not encrypted');

$result = $node->safe_psql('postgres', q{SELECT a, b, c FROM tbl1});
is($result,
	q(1|val1|11
2|val2|22),
	'decrypted query result after test client insert');

$node->command_ok(['test_client', 'test2'], 'test client test 2');

$result = $node->safe_psql('postgres', q{SELECT a, b, c FROM tbl1});
is($result,
	q(1|val1|11
2|val2|22
3|val3|33),
	'decrypted query result after test client insert 2');

like($node->safe_psql('postgres', q{COPY (SELECT * FROM tbl1 WHERE a = 3) TO STDOUT}),
	qr/3\t\\\\x[0-9a-f]{96}/,
	'inserted data is encrypted');

$node->command_ok(['test_client', 'test3'], 'test client test 3');

$result = $node->safe_psql('postgres', q{SELECT a, b, c FROM tbl1});
is($result,
	q(1|val1|11
2|val2|22
3|val3upd|33),
	'decrypted query result after test client insert 3');

$node->command_ok(['test_client', 'test4'], 'test client test 4');

$result = $node->safe_psql('postgres', q{SELECT a, b FROM tbl2});
is($result,
	q(1|valA
2|valB
3|valA),
	'decrypted query result after test client insert 4');

is($node->safe_psql('postgres', q{SELECT b, count(*) FROM tbl2 GROUP BY b ORDER BY 2}),
	q(valB|1
valA|2),
	'group by deterministically encrypted column');

$node->command_ok(['test_client', 'test5'], 'test client test 5');

$result = $node->safe_psql('postgres', q{SELECT a, b, c FROM tbl3});
is($result,
	q(1|valB1|valC1
2|valB2|valC2
3|valB3|valC3),
	'decrypted query result multiple keys after test client insert 5');

done_testing();
