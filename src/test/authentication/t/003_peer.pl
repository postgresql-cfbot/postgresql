
# Copyright (c) 2022, PostgreSQL Global Development Group

# Tests for peer authentication and user name map.
# The test is skipped if the platform does not support peer authentication.

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
	# just for testing purposes, use a continuation line.
	$node->append_conf('pg_hba.conf', "local all all\\\n $hba_method");
	$node->reload;
	return;
}

# Test access for a single role, useful to wrap all tests into one.
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

# find $pat in logfile of $node.
sub find_in_log
{
  my ($node, $pat) = @_;

  my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile);
  return 0 if (length($log) <= 0);

  return $log =~ m/$pat/;
}

# Initialize primary node.
my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;
$node->append_conf('postgresql.conf', "log_connections = on\n");
$node->start;

# Set pg_hba.conf with the peer authentication.
reset_pg_hba($node, 'peer');

# Check that peer authentication is supported on this platform.
$node->psql('postgres');

# If not supported, then skip the rest of the test.
if (find_in_log($node, qr/peer authentication is not supported on this platform/))
{
    plan skip_all => 'peer authentication is not supported on this platform';
}

# Let's create a new user for the user name map test.
$node->safe_psql('postgres',
  qq{CREATE USER testmapuser});

# Get the system_user to define the user name map test.
my $system_user =
  $node->safe_psql('postgres', q(select (string_to_array(SYSTEM_USER, ':'))[2]));

# This connection should succeed.
test_role($node, $system_user, 'peer', 0,
    log_like =>
      [qr/connection authenticated: identity="$system_user" method=peer/]);

# This connection should failed.
test_role($node, qq{testmapuser}, 'peer', 2,
    log_like => [qr/Peer authentication failed for user "testmapuser"/]);

# Define a user name map.
$node->append_conf('pg_ident.conf', qq{mypeermap $system_user testmapuser});

# Set pg_hba.conf with the peer authentication and the user name map.
reset_pg_hba($node, 'peer map=mypeermap');

# This connection should now succeed.
test_role($node, qq{testmapuser}, 'peer', 0,
    log_like =>
      [qr/connection authenticated: identity="$system_user" method=peer/]);

done_testing();
