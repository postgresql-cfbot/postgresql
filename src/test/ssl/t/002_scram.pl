# Test SCRAM authentication and TLS channel binding types

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

use File::Copy;

use FindBin;
use lib $FindBin::RealBin;

use SSL::Server;

my $openssl;
my $nss;

my $supports_tls_server_end_point;

if ($ENV{with_ssl} eq 'no')
{
	plan skip_all => 'SSL not supported by this build';
}
else
{
	if ($ENV{with_ssl} eq 'openssl')
	{
		$openssl = 1;
		# Determine whether build supports tls-server-end-point.
		$supports_tls_server_end_point =
			check_pg_config("#define HAVE_X509_GET_SIGNATURE_NID 1");
		plan tests => $supports_tls_server_end_point ? 9 : 10;
	}
	elsif ($ENV{with_ssl} eq 'nss')
	{
		$nss = 1;
		plan tests => 8;
	}
}

# This is the hostname used to connect to the server.
my $SERVERHOSTADDR = '127.0.0.1';
# This is the pattern to use in pg_hba.conf to match incoming connections.
my $SERVERHOSTCIDR = '127.0.0.1/32';

# Allocation of base connection string shared among multiple tests.
my $common_connstr;

# Set up the server.

note "setting up data directory";
my $node = get_new_node('primary');
$node->init;

# PGHOST is enforced here to set up the node, subsequent connections
# will use a dedicated connection string.
$ENV{PGHOST} = $node->host;
$ENV{PGPORT} = $node->port;
$node->start;

# Configure server for SSL connections, with password handling.
configure_test_server_for_ssl($node, $SERVERHOSTADDR, $SERVERHOSTCIDR,
	"scram-sha-256", "pass", "scram-sha-256");
switch_server_cert($node, 'server-cn-only');
$ENV{PGPASSWORD} = "pass";
$common_connstr =
  "dbname=trustdb sslmode=require sslcert=invalid sslrootcert=invalid hostaddr=$SERVERHOSTADDR";

# Default settings
test_connect_ok($common_connstr, "user=ssltestuser",
	"Basic SCRAM authentication with SSL");

# Test channel_binding
test_connect_fails(
	$common_connstr,
	"user=ssltestuser channel_binding=invalid_value",
	qr/invalid channel_binding value: "invalid_value"/,
	"SCRAM with SSL and channel_binding=invalid_value");
test_connect_ok(
	$common_connstr,
	"user=ssltestuser channel_binding=disable",
	"SCRAM with SSL and channel_binding=disable");

SKIP:
{
	skip "NSS support for TLS server endpoing is not implemented", 1 if ($nss);
	if ($supports_tls_server_end_point)
	{
		test_connect_ok(
			$common_connstr,
			"user=ssltestuser channel_binding=require",
			"SCRAM with SSL and channel_binding=require");
	}
	else
	{
		test_connect_fails(
			$common_connstr,
			"user=ssltestuser channel_binding=require",
			qr/channel binding is required, but server did not offer an authentication method that supports channel binding/,
			"SCRAM with SSL and channel_binding=require");
	}
}

# Now test when the user has an MD5-encrypted password; should fail
test_connect_fails(
	$common_connstr,
	"user=md5testuser channel_binding=require",
	qr/channel binding required but not supported by server's authentication request/,
	"MD5 with SSL and channel_binding=require");

SKIP:
{
	skip "NSS support not implemented yet", 1 if ($nss);
	# Now test with auth method 'cert' by connecting to 'certdb'. Should fail,
	# because channel binding is not performed.  Note that ssl/client.key may
	# be used in a different test, so the name of this temporary client key
	# is chosen here to be unique.
	my $client_tmp_key = "ssl/client_scram_tmp.key";
	copy("ssl/client.key", $client_tmp_key);
	chmod 0600, $client_tmp_key;
	test_connect_fails(
		"sslcert=ssl/client.crt sslkey=$client_tmp_key sslrootcert=invalid hostaddr=$SERVERHOSTADDR",
		"dbname=certdb user=ssltestuser channel_binding=require",
		qr/channel binding required, but server authenticated client without channel binding/,
		"Cert authentication and channel_binding=require");
	# clean up
	unlink($client_tmp_key);
}
