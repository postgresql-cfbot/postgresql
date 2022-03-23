
# Copyright (c) 2021-2022, PostgreSQL Global Development Group

# Set of tests for testing custom authentication hooks.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Delete pg_hba.conf from the given node, add a new entry to it
# and then execute a reload to refresh it.
sub reset_pg_hba
{
	my $node       = shift;
	my $hba_method = shift;

	unlink($node->data_dir . '/pg_hba.conf');
	# just for testing purposes, use a continuation line
	$node->append_conf('pg_hba.conf', "local all all\\\n $hba_method");
	$node->reload;
	return;
}

# Test if you get expected results in pg_hba_file_rules error column after
# changing pg_hba.conf and reloading it.
sub test_hba_reload
{
	my ($node, $method, $expected_res) = @_;
	my $status_string = 'failed';
	$status_string = 'success' if ($expected_res eq 0);
	my $testname = "pg_hba.conf reload $status_string for method $method";

	reset_pg_hba($node, $method);

	my ($cmdret, $stdout, $stderr) = $node->psql("postgres",
		"select count(*) from pg_hba_file_rules where error is not null",extra_params => ['-U','bob']);

	is($stdout, $expected_res, $testname);
}

# Test access for a single role, useful to wrap all tests into one.  Extra
# named parameters are passed to connect_ok/fails as-is.
sub test_role
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	my ($node, $role, $method, $expected_res, %params) = @_;
	my $status_string = 'failed';
	$status_string = 'success' if ($expected_res eq 0);

	my $connstr = "user=$role";
	my $testname =
	  "authentication $status_string for method $method, role $role";

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

# Initialize server node
my $node = PostgreSQL::Test::Cluster->new('server');
$node->init;
$node->append_conf('postgresql.conf', "log_connections = on\n");
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'test_auth_provider.so'\n");
$node->start;

$node->safe_psql('postgres', "CREATE ROLE bob SUPERUSER LOGIN;");
$node->safe_psql('postgres', "CREATE ROLE alice LOGIN;");
$node->safe_psql('postgres', "CREATE ROLE test LOGIN;");

# Add custom auth method to pg_hba.conf
reset_pg_hba($node, 'custom provider=test');

# Test that users are able to login with correct passwords.
$ENV{"PGPASSWORD"} = 'bob123';
test_role($node, 'bob', 'custom', 0, log_like => [qr/connection authorized: user=bob/]);
$ENV{"PGPASSWORD"} = 'alice123';
test_role($node, 'alice', 'custom', 0, log_like => [qr/connection authorized: user=alice/]);

# Test that bad passwords are rejected.
$ENV{"PGPASSWORD"} = 'badpassword';
test_role($node, 'bob', 'custom', 2, log_unlike => [qr/connection authorized:/]);
test_role($node, 'alice', 'custom', 2, log_unlike => [qr/connection authorized:/]);

# Test that users not in authentication list are rejected.
test_role($node, 'test', 'custom', 2, log_unlike => [qr/connection authorized:/]);

$ENV{"PGPASSWORD"} = 'bob123';

# Tests for invalid auth options

# Test that an incorrect provider name is not accepted.
test_hba_reload($node, 'custom provider=wrong', 1);

# Test that specifying provider option with different auth method is not allowed.
test_hba_reload($node, 'trust provider=test', 1);

# Test that provider name is a mandatory option for custom auth.
test_hba_reload($node, 'custom', 1);

# Test that correct provider name allows reload to succeed.
test_hba_reload($node, 'custom provider=test', 0);

# Tests for custom auth options

# Test that a custom option doesn't work without a provider.
test_hba_reload($node, 'custom allow=bob', 1);

# Test that options other than allowed ones are not accepted.
test_hba_reload($node, 'custom provider=test wrong=true', 1);

# Test that only valid values are accepted for allowed options.
test_hba_reload($node, 'custom provider=test allow=wrong', 1);

# Test that setting allow option for a user doesn't look at the password.
test_hba_reload($node, 'custom provider=test allow=bob', 0);
$ENV{"PGPASSWORD"} = 'bad123';
test_role($node, 'bob', 'custom', 0, log_like => [qr/connection authorized: user=bob/]);

# Password is still checked for other users.
test_role($node, 'alice', 'custom', 2, log_unlike => [qr/connection authorized:/]);

# Reset the password for future tests.
$ENV{"PGPASSWORD"} = 'bob123';

# Custom auth modules require mentioning extension in shared_preload_libraries.

# Remove extension from shared_preload_libraries and try to restart.
$node->adjust_conf('postgresql.conf', 'shared_preload_libraries', "''");
command_fails(['pg_ctl', '-w', '-D', $node->data_dir, '-l', $node->logfile, 'restart'],'restart with empty shared_preload_libraries failed');

# Fix shared_preload_libraries and confirm that you can now restart.
$node->adjust_conf('postgresql.conf', 'shared_preload_libraries', "'test_auth_provider.so'");
command_ok(['pg_ctl', '-w', '-D', $node->data_dir, '-l', $node->logfile,'start'],'restart with correct shared_preload_libraries succeeded');

# Test that we can connect again
test_role($node, 'bob', 'custom', 0, log_like => [qr/connection authorized: user=bob/]);

done_testing();
