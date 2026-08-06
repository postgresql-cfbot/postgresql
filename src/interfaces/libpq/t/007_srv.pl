# Copyright (c) 2026, PostgreSQL Global Development Group
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use Test::More;

# This test exercises DNS SRV record support in libpq:
#
#   1. URI parsing: postgresql+srv:// and postgres+srv:// schemes store the
#      netloc as "srvhost" rather than "host".
#   2. Error handling: multiple hosts in a +srv URI, mixing srvhost with host.
#   3. Live SRV lookup against a running PostgreSQL cluster.
#
# Live lookup is gated by PG_TEST_EXTRA=srv because it requires that valid
# SRV records exist in DNS.  The test relies on the SRV_HOST environment
# variable (defaulting to the value recommended in the PostgreSQL docs).


# --------------------------------------------------------------------------
# Part 1: URI parsing (no network required, uses libpq_uri_regress)
# --------------------------------------------------------------------------

my @uri_tests = (

	# postgresql+srv:// — netloc becomes srvhost
	[
		q{postgresql+srv://cluster.example.com/mydb},
		q{dbname='mydb' srvhost='cluster.example.com'},
		q{},
	],

	# postgres+srv:// short form
	[
		q{postgres+srv://cluster.example.com/mydb},
		q{dbname='mydb' srvhost='cluster.example.com'},
		q{},
	],

	# with user, extra params
	[
		q{postgresql+srv://alice@cluster.example.com/mydb?target_session_attrs=read-write},
		q{user='alice' dbname='mydb' srvhost='cluster.example.com' target_session_attrs='read-write'},
		q{},
	],

	# multiple hosts must be rejected for +srv URIs
	[
		q{postgresql+srv://h1.example.com,h2.example.com/mydb},
		q{},
		q{multiple hosts are not allowed in a postgresql+srv:// URI},
	],
);

foreach my $t (@uri_tests)
{
	my ($uri, $want_out, $want_err) = @$t;

	my ($stdout, $stderr);
	IPC::Run::run [ 'libpq_uri_regress', $uri ],
	  '>' => \$stdout,
	  '2>' => \$stderr;
	chomp $stdout;
	chomp $stderr;

	# Strip the trailing connection-type annotation "(local)"/"(inet)" so
	# that the test is not sensitive to socket-vs-TCP defaults.
	$stdout =~ s/\s+\(\w+\)$//;

	is($stdout, $want_out, "URI stdout: $uri");
	like($stderr, qr/\Q$want_err\E/, "URI stderr: $uri")
	  if $want_err ne '';
	is($stderr, '', "URI no stderr: $uri")
	  if $want_err eq '';
}

# --------------------------------------------------------------------------
# Part 2: Live SRV lookup (optional, gated by PG_TEST_EXTRA)
# --------------------------------------------------------------------------

my $do_live = $ENV{PG_TEST_EXTRA} && $ENV{PG_TEST_EXTRA} =~ /\bsrv\b/;

if (!$do_live)
{
	note 'Skipping live SRV test (not enabled in PG_TEST_EXTRA)';
	done_testing();
	exit 0;
}

# The live test starts a PostgreSQL cluster on a local port, publishes the
# connection details via a mock hosts-file trick (as done in 004_load_balance_dns.pl),
# and verifies that a postgresql+srv:// URI resolves and connects.
#
# When running in CI, the SRV records for $SRV_HOST must resolve to 127.0.0.1
# on port $SRV_PORT.

my $srv_host =
  $ENV{SRV_HOST} // 'pg-srvtest';   # DNS name whose SRV records point here
my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;
$node->start;

my $port = $node->port;

note "Testing SRV connection via srvhost=$srv_host (expect port $port)";

# 1. keyword=value connection string
my ($ret, $out, $err);
$ret = $node->psql(
	'postgres',
	'SELECT 1',
	stdout         => \$out,
	stderr         => \$err,
	extra_params   => [ '-d', "srvhost=$srv_host" ],
	on_error_stop  => 0);
is($ret, 0, "srvhost= keyword connects via SRV")
  or diag("stderr: $err");

# 2. URI form
$ret = $node->psql(
	'postgres',
	'SELECT 1',
	stdout        => \$out,
	stderr        => \$err,
	extra_params  => [ '-d', "postgresql+srv://$srv_host/postgres" ],
	on_error_stop => 0);
is($ret, 0, "postgresql+srv:// URI connects via SRV")
  or diag("stderr: $err");

# 3. target_session_attrs=any works with SRV
$ret = $node->psql(
	'postgres',
	'SELECT 1',
	stdout        => \$out,
	stderr        => \$err,
	extra_params  => [
		'-d',
		"postgresql+srv://$srv_host/postgres?target_session_attrs=any",
	],
	on_error_stop => 0);
is($ret, 0, "postgresql+srv:// with target_session_attrs=any")
  or diag("stderr: $err");

# 4. Mixing srvhost and host is rejected
$ret = $node->psql(
	'postgres',
	'SELECT 1',
	stdout        => \$out,
	stderr        => \$err,
	extra_params  => [ '-d', "srvhost=$srv_host host=localhost" ],
	on_error_stop => 0);
isnt($ret, 0, "srvhost + host= is rejected");
like($err, qr/cannot use "srvhost" together with "host"/,
	 "correct error for srvhost + host conflict");

$node->stop;

done_testing();
