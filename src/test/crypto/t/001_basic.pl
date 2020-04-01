use strict;
use warnings;
use TestLib;
use PostgresNode;
use Test::More tests => 8;

my $node = get_new_node('node');
$node->init(enable_kms => 1);
$node->start;

sub test_wrap
{
	my ($node, $data, $test_name) = @_;

	my $res = $node->safe_psql(
		'postgres',
		qq(
		SELECT pg_unwrap(pg_wrap('$data'));
		)
	  );
	is($res, $data, $test_name);
}

# Control file should know that checksums are disabled.
command_like(
	[ 'pg_controldata', $node->data_dir ],
	qr/Key management:.*on/,
	'key manager is enabled in control file');

test_wrap($node, '123456', 'less block size');
test_wrap($node, '1234567890123456', 'one block size');
test_wrap($node, '12345678901234567890', 'more than one block size');

# Get the token wrapped by the encryption key
my $token = 'test_token';
my $wrapped_token = $node->safe_psql('postgres',
									 qq(SELECT pg_wrap('$token')));
# Change the cluster passphrase command
$node->safe_psql('postgres',
				 qq(ALTER SYSTEM SET cluster_passphrase_command =
				 'echo 1234123456789012345678901234567890123456789012345678901234567890';));
$node->reload;

my $ret = $node->safe_psql('postgres', 'SELECT pg_rotate_cluster_passphrase()');
is($ret, 't', 'cluster passphrase rotation');

$node->restart;

# Unwrap the token after passphrase rotation.
my $ret_token = $node->safe_psql('postgres',
								 qq(SELECT pg_unwrap('$wrapped_token')));
is($ret_token, $token, 'unwrap after passphrase rotation');
