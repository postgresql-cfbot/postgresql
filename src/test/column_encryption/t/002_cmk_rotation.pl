# Copyright (c) 2021-2023, PostgreSQL Global Development Group

# Test column master key rotation.  First, we generate CMK1 and a CEK
# encrypted with it.  Then we add a CMK2 and encrypt the CEK with it
# as well.  (Recall that a CEK can be associated with multiple CMKs,
# for this reason.  That's why pg_colenckeydata is split out from
# pg_colenckey.)  Then we remove CMK1.  We test that we can get
# decrypted query results at each step.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $openssl = $ENV{OPENSSL};

my $cmkalg = 'RSAES_OAEP_SHA_1';

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;


sub create_cmk
{
	my ($cmkname) = @_;
	my $cmkfilename = "${PostgreSQL::Test::Utils::tmp_check}/${cmkname}.pem";
	system_or_bail $openssl, 'genpkey', '-algorithm', 'rsa', '-out', $cmkfilename;
	$node->safe_psql('postgres', qq{CREATE COLUMN MASTER KEY ${cmkname}});
	return $cmkfilename;
}


my $cmk1filename = create_cmk('cmk1');

# create CEK
my ($cekname, $bytes) = ('cek1', 48);

# generate random bytes
system_or_bail $openssl, 'rand', '-out', "${PostgreSQL::Test::Utils::tmp_check}/${cekname}.bin", $bytes;

# encrypt CEK using CMK
system_or_bail $openssl, 'pkeyutl', '-encrypt',
  '-inkey', $cmk1filename,
  '-pkeyopt', 'rsa_padding_mode:oaep',
  '-in', "${PostgreSQL::Test::Utils::tmp_check}/${cekname}.bin",
  '-out', "${PostgreSQL::Test::Utils::tmp_check}/${cekname}.bin.enc";

my $cekenchex = unpack('H*', slurp_file "${PostgreSQL::Test::Utils::tmp_check}/${cekname}.bin.enc");

# create CEK in database
$node->safe_psql('postgres', qq{CREATE COLUMN ENCRYPTION KEY ${cekname} WITH VALUES (column_master_key = cmk1, algorithm = '$cmkalg', encrypted_value = '\\x${cekenchex}');});

$ENV{PGCOLUMNENCRYPTION} = 'on';
$ENV{PGCMKLOOKUP} = '*=file:' . ${PostgreSQL::Test::Utils::tmp_check} . '/%k.pem';

$node->safe_psql('postgres', qq{
CREATE TABLE tbl1 (
    a int,
    b text ENCRYPTED WITH (column_encryption_key = cek1)
);
});

$node->safe_psql('postgres', q{
INSERT INTO tbl1 (a, b) VALUES (1, $1) \bind 'val1' \g
INSERT INTO tbl1 (a, b) VALUES (2, $1) \bind 'val2' \g
});

is($node->safe_psql('postgres', q{SELECT a, b FROM tbl1}),
	q(1|val1
2|val2),
	'decrypted query result with one CMK');


# create new CMK
my $cmk2filename = create_cmk('cmk2');

# encrypt CEK using new CMK
#
# (Here, we still have the plaintext of the CEK available from
# earlier.  In reality, one would decrypt the CEK with the first CMK
# and then re-encrypt it with the second CMK.)
system_or_bail $openssl, 'pkeyutl', '-encrypt',
  '-inkey', $cmk2filename,
  '-pkeyopt', 'rsa_padding_mode:oaep',
  '-in', "${PostgreSQL::Test::Utils::tmp_check}/${cekname}.bin",
  '-out', "${PostgreSQL::Test::Utils::tmp_check}/${cekname}.bin.enc";

$cekenchex = unpack('H*', slurp_file "${PostgreSQL::Test::Utils::tmp_check}/${cekname}.bin.enc");

# add new data record for CEK in database
$node->safe_psql('postgres', qq{ALTER COLUMN ENCRYPTION KEY ${cekname} ADD VALUE (column_master_key = cmk2, algorithm = '$cmkalg', encrypted_value = '\\x${cekenchex}');});


is($node->safe_psql('postgres', q{SELECT a, b FROM tbl1}),
	q(1|val1
2|val2),
	'decrypted query result with two CMKs');


# delete CEK record for first CMK
$node->safe_psql('postgres', qq{ALTER COLUMN ENCRYPTION KEY ${cekname} DROP VALUE (column_master_key = cmk1);});


is($node->safe_psql('postgres', q{SELECT a, b FROM tbl1}),
	q(1|val1
2|val2),
	'decrypted query result with only new CMK');


done_testing();
