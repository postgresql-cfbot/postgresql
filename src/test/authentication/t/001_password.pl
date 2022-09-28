
# Copyright (c) 2021-2022, PostgreSQL Global Development Group

# Set of tests for authentication and pg_hba.conf. The following password
# methods are checked through this test:
# - Plain
# - MD5-encrypted
# - SCRAM-encrypted
# This test can only run with Unix-domain sockets.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
if (!$use_unix_sockets)
{
	plan skip_all =>
	  "authentication tests cannot run without Unix-domain sockets";
}

# Delete pg_hba.conf from the given node, add a new entry to it
# and then execute a reload to refresh it.
sub reset_pg_hba
{
	my $node		= shift;
	my $host		= shift;
	my $database	= shift;
	my $role		= shift;
	my $method		= shift;

	unlink($node->data_dir . '/pg_hba.conf');
	# just for testing purposes, use a continuation line
	$node->append_conf('pg_hba.conf', "$host $database $role\\\n $method");
	$node->reload;
	return;
}

# Test access for a single role, useful to wrap all tests into one.  Extra
# named parameters are passed to connect_ok/fails as-is.
sub test_conn
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	my ($node, $conn, $method, $expected_res, %params) = @_;
	my $status_string = 'failed';
	$status_string = 'success' if ($expected_res eq 0);

	my $connstr = "$conn";
	my $testname =
	  "authentication $status_string for method $method, conn $conn";

	if ($expected_res eq 0)
	{
		$node->connect_ok($connstr, $testname, %params);
	}
	else
	{
		# No checks of the error message, only the status code.
		$node->connect_fails($connstr, $testname, %params);
	}
}

# Initialize primary node
my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;
$node->append_conf(
    'postgresql.conf', qq{
listen_addresses = '127.0.0.1'
log_connections = on
});
$node->start;

# Create 3 roles with different password methods for each one. The same
# password is used for all of them.
$node->safe_psql('postgres',
	"SET password_encryption='scram-sha-256'; CREATE ROLE scram_role LOGIN PASSWORD 'pass';"
);
$node->safe_psql('postgres',
	"SET password_encryption='md5'; CREATE ROLE md5_role LOGIN PASSWORD 'pass';"
);
$ENV{"PGPASSWORD"} = 'pass';

# Create a database to test regular expression
$node->safe_psql('postgres',
	"CREATE database testdb;"
);

# For "trust" method, all users should be able to connect. These users are not
# considered to be authenticated.
reset_pg_hba($node, 'local','all', 'all', 'trust');
test_conn($node, 'user=scram_role', 'trust', 0,
	log_unlike => [qr/connection authenticated:/]);
test_conn($node, 'user=md5_role', 'trust', 0,
	log_unlike => [qr/connection authenticated:/]);

# For plain "password" method, all users should also be able to connect.
reset_pg_hba($node, 'local', 'all', 'all', 'password');
test_conn($node, 'user=scram_role', 'password', 0,
	log_like =>
	  [qr/connection authenticated: identity="scram_role" method=password/]);
test_conn($node, 'user=md5_role', 'password', 0,
	log_like =>
	  [qr/connection authenticated: identity="md5_role" method=password/]);

# For "scram-sha-256" method, user "scram_role" should be able to connect.
reset_pg_hba($node, 'local', 'all', 'all', 'scram-sha-256');
test_conn(
	$node,
	'user=scram_role',
	'scram-sha-256',
	0,
	log_like => [
		qr/connection authenticated: identity="scram_role" method=scram-sha-256/
	]);
test_conn($node, 'user=md5_role', 'scram-sha-256', 2,
	log_unlike => [qr/connection authenticated:/]);

# Test that bad passwords are rejected.
$ENV{"PGPASSWORD"} = 'badpass';
test_conn($node, 'user=scram_role', 'scram-sha-256', 2,
	log_unlike => [qr/connection authenticated:/]);
$ENV{"PGPASSWORD"} = 'pass';

# For "md5" method, all users should be able to connect (SCRAM
# authentication will be performed for the user with a SCRAM secret.)
reset_pg_hba($node, 'local', 'all', 'all', 'md5');
test_conn($node, 'user=scram_role', 'md5', 0,
	log_like =>
	  [qr/connection authenticated: identity="scram_role" method=md5/]);
test_conn($node, 'user=md5_role', 'md5', 0,
	log_like =>
	  [qr/connection authenticated: identity="md5_role" method=md5/]);

# Tests for channel binding without SSL.
# Using the password authentication method; channel binding can't work
reset_pg_hba($node, 'local', 'all', 'all', 'password');
$ENV{"PGCHANNELBINDING"} = 'require';
test_conn($node, 'user=scram_role', 'scram-sha-256', 2);
# SSL not in use; channel binding still can't work
reset_pg_hba($node, 'local', 'all', 'all', 'scram-sha-256');
$ENV{"PGCHANNELBINDING"} = 'require';
test_conn($node, 'user=scram_role', 'scram-sha-256', 2);

# Test .pgpass processing; but use a temp file, don't overwrite the real one!
my $pgpassfile = "${PostgreSQL::Test::Utils::tmp_check}/pgpass";

delete $ENV{"PGPASSWORD"};
delete $ENV{"PGCHANNELBINDING"};
$ENV{"PGPASSFILE"} = $pgpassfile;

unlink($pgpassfile);
append_to_file(
	$pgpassfile, qq!
# This very long comment is just here to exercise handling of long lines in the file. This very long comment is just here to exercise handling of long lines in the file. This very long comment is just here to exercise handling of long lines in the file. This very long comment is just here to exercise handling of long lines in the file. This very long comment is just here to exercise handling of long lines in the file.
*:*:postgres:scram_role:pass:this is not part of the password.
!);
chmod 0600, $pgpassfile or die;

reset_pg_hba($node, 'local', 'all', 'all', 'password');
test_conn($node, 'user=scram_role', 'password from pgpass', 0);
test_conn($node, 'user=md5_role',   'password from pgpass', 2);

append_to_file(
	$pgpassfile, qq!
*:*:*:md5_role:p\\ass
!);

test_conn($node, 'user=md5_role', 'password from pgpass', 0);

# Testing with regular expression for username
reset_pg_hba($node, 'local', 'all', '/^.*nomatch.*$, baduser, /^.*md.*$', 'password');
test_conn($node, 'user=md5_role', 'password, matching regexp for username', 0);

reset_pg_hba($node, 'local', 'all', '/^.*nomatch.*$, baduser, /^.*m_d.*$', 'password');
test_conn($node, 'user=md5_role', 'password, non matching regexp for username', 2,
		log_unlike => [qr/connection authenticated:/]);

# Testing with regular expression for dbname
reset_pg_hba($node, 'local', '/^.*nomatch.*$, baddb, /^t.*b$', 'all', 'password');
test_conn($node, 'user=md5_role dbname=testdb', 'password, matching regexp for dbname', 0);

reset_pg_hba($node, 'local', '/^.*nomatch.*$, baddb, /^t.*ba$', 'all', 'password');
test_conn($node, 'user=md5_role dbname=testdb', 'password, non matching regexp for dbname', 2,
		log_unlike => [qr/connection authenticated:/]);

# Testing with regular expression for hostname
SKIP:
{
	# Being able to do a reverse lookup of a hostname on Windows for localhost
	# is not guaranteed on all environments by default.
	# So, skip the regular expression test for hostname on Windows.
	skip "Regular expression for hostname not tested on Windows", 2 if ($windows_os);

	reset_pg_hba($node, 'host', 'all', 'all  /^.*$', 'password');
	test_conn($node, 'user=md5_role host=localhost', 'password, matching regexp for hostname', 0);

	reset_pg_hba($node, 'host', 'all', 'all  /^$', 'password');
	test_conn($node, 'user=md5_role host=localhost', 'password, non matching regexp for hostname', 2);
}
done_testing();
