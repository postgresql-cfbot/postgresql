
# Copyright (c) 2021, PostgreSQL Global Development Group

# Test SCRAM authentication and TLS channel binding types

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use File::Copy;

use FindBin;
use lib $FindBin::RealBin;

use SSL::Server;

my $openssl;
my $nss;

my $supports_tls_server_end_point;

if ($ENV{with_ssl} eq 'openssl')
{
	$openssl = 1;
	# Determine whether build supports tls-server-end-point.
	$supports_tls_server_end_point =
		check_pg_config("#define HAVE_X509_GET_SIGNATURE_NID 1");
	plan tests => 15;
}
elsif ($ENV{with_ssl} eq 'nss')
{
	$nss = 1;
	$supports_tls_server_end_point = 1;
	plan tests => 15;
}
else
{
	plan skip_all => 'SSL not supported by this build';
}

# This is the hostname used to connect to the server.
my $SERVERHOSTADDR = '127.0.0.1';
# This is the pattern to use in pg_hba.conf to match incoming connections.
my $SERVERHOSTCIDR = '127.0.0.1/32';

# Allocation of base connection string shared among multiple tests.
my $common_connstr;

# Set up the server.

note "setting up data directory";
my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;

# PGHOST is enforced here to set up the node, subsequent connections
# will use a dedicated connection string.
$ENV{PGHOST} = $node->host;
$ENV{PGPORT} = $node->port;
$node->start;

# Configure server for SSL connections, with password handling.
configure_test_server_for_ssl($node, $SERVERHOSTADDR, $SERVERHOSTCIDR,
	"scram-sha-256", "pass", "scram-sha-256");
switch_server_cert($node, certfile => 'server-cn-only', nssdatabase => 'server-cn-only.crt__server-cn-only.key.db');
$ENV{PGPASSWORD} = "pass";
$common_connstr =
  "dbname=trustdb sslmode=require sslcert=invalid sslrootcert=invalid hostaddr=$SERVERHOSTADDR";

# Default settings
$node->connect_ok(
	"$common_connstr user=ssltestuser",
	"Basic SCRAM authentication with SSL");

# Test channel_binding
$node->connect_fails(
	"$common_connstr user=ssltestuser channel_binding=invalid_value",
	"SCRAM with SSL and channel_binding=invalid_value",
	expected_stderr => qr/invalid channel_binding value: "invalid_value"/);
$node->connect_ok("$common_connstr user=ssltestuser channel_binding=disable",
	"SCRAM with SSL and channel_binding=disable");
if ($supports_tls_server_end_point)
{
	$node->connect_ok(
		"$common_connstr user=ssltestuser channel_binding=require",
		"SCRAM with SSL and channel_binding=require");
}
else
{
	$node->connect_fails(
		"$common_connstr user=ssltestuser channel_binding=require",
		"SCRAM with SSL and channel_binding=require",
		expected_stderr =>
		  qr/channel binding is required, but server did not offer an authentication method that supports channel binding/
	);
}

# Now test when the user has an MD5-encrypted password; should fail
$node->connect_fails(
	"$common_connstr user=md5testuser channel_binding=require",
	"MD5 with SSL and channel_binding=require",
	expected_stderr =>
	  qr/channel binding required but not supported by server's authentication request/
);

# Now test with auth method 'cert' by connecting to 'certdb'. Should fail,
# because channel binding is not performed.  Note that ssl/client.key may
# be used in a different test, so the name of this temporary client key
# is chosen here to be unique.
my $client_tmp_key = "ssl/client_scram_tmp.key";
copy("ssl/client.key", $client_tmp_key);
chmod 0600, $client_tmp_key;
$node->connect_fails(
	"sslcert=ssl/client.crt sslkey=$client_tmp_key sslrootcert=invalid hostaddr=$SERVERHOSTADDR ssldatabase=ssl/nss/client.crt__client.key.db dbname=certdb user=ssltestuser channel_binding=require",
	"Cert authentication and channel_binding=require",
	expected_stderr =>
	  qr/channel binding required, but server authenticated client without channel binding/
);

# Certificate verification at the connection level should still work fine.
$node->connect_ok(
	"sslcert=ssl/client.crt sslkey=$client_tmp_key sslrootcert=invalid hostaddr=$SERVERHOSTADDR ssldatabase=ssl/nss/client.crt__client.key.db dbname=verifydb user=ssltestuser",
	"SCRAM with clientcert=verify-full",
	log_like => [
		qr/connection authenticated: identity="ssltestuser" method=scram-sha-256/
	]);

# clean up
unlink($client_tmp_key);
