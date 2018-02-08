use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;
use ServerSetup;
use File::Copy;

#### Some configuration

# This is the hostname used to connect to the server. This cannot be a
# hostname, because the server certificate is always for the domain
# postgresql-ssl-regression.test.
my $SERVERHOSTADDR = '127.0.0.1';

# Allocation of base connection string shared among multiple tests.
my $common_connstr;

# The client's private key must not be world-readable, so take a copy
# of the key stored in the code tree and update its permissions.
copy("ssl/client.key", "ssl/client_tmp.key");
chmod 0600, "ssl/client_tmp.key";

# macOS Secure Transport does not support loading CRL files, so skip CRL
# tests when built with Secure Transport support.
my $supports_crl_files = ! check_pg_config("#define USE_SECURETRANSPORT 1");

# Test for support of Secure Transport Keychain secure archives
# This check is currently the same as $supports_crl_files, but clarity when
# reading code is more important than avoiding the use of an extra variable
my $supports_keychains = check_pg_config("#define USE_SECURETRANSPORT 1");

#### Setup

if ($supports_keychains)
{
	plan tests => 43;
}
else
{
	plan tests => 40;
}


#### Part 0. Set up the server.

note "setting up data directory";
my $node = get_new_node('master');
$node->init;

# PGHOST is enforced here to set up the node, subsequent connections
# will use a dedicated connection string.
$ENV{PGHOST} = $node->host;
$ENV{PGPORT} = $node->port;
$node->start;
configure_test_server_for_ssl($node, $SERVERHOSTADDR, 'trust');
switch_server_cert($node, 'server-cn-only');

### Part 1. Run client-side tests.
###
### Test that libpq accepts/rejects the connection correctly, depending
### on sslmode and whether the server's certificate looks correct. No
### client certificate is used in these tests.

note "running client tests";

$common_connstr =
"user=ssltestuser dbname=trustdb sslcert=invalid hostaddr=$SERVERHOSTADDR host=common-name.pg-ssltest.test";

# The server should not accept non-SSL connections.
note "test that the server doesn't accept non-SSL connections";
test_connect_fails($common_connstr, "sslmode=disable");

# Try without a root cert. In sslmode=require, this should work. In verify-ca
# or verify-full mode it should fail.
note "connect without server root cert";
test_connect_ok($common_connstr, "sslrootcert=invalid sslmode=require");
test_connect_fails($common_connstr, "sslrootcert=invalid sslmode=verify-ca");
test_connect_fails($common_connstr, "sslrootcert=invalid sslmode=verify-full");

# Try with wrong root cert, should fail. (We're using the client CA as the
# root, but the server's key is signed by the server CA.)
note "connect with wrong server root cert";
test_connect_fails($common_connstr,
	"sslrootcert=ssl/client_ca.crt sslmode=require");
test_connect_fails($common_connstr,
	"sslrootcert=ssl/client_ca.crt sslmode=verify-ca");
test_connect_fails($common_connstr,
	"sslrootcert=ssl/client_ca.crt sslmode=verify-full");

# Try with just the server CA's cert. This fails because the root file
# must contain the whole chain up to the root CA.
note "connect with server CA cert, without root CA";
test_connect_fails($common_connstr,
	"sslrootcert=ssl/server_ca.crt sslmode=verify-ca");

# And finally, with the correct root cert.
note "connect with correct server CA cert file";
test_connect_ok($common_connstr,
	"sslrootcert=ssl/root+server_ca.crt sslmode=require");
test_connect_ok($common_connstr,
	"sslrootcert=ssl/root+server_ca.crt sslmode=verify-ca");
test_connect_ok($common_connstr,
	"sslrootcert=ssl/root+server_ca.crt sslmode=verify-full");

# Test with cert root file that contains two certificates. The client should
# be able to pick the right one, regardless of the order in the file.
test_connect_ok($common_connstr,
	"sslrootcert=ssl/both-cas-1.crt sslmode=verify-ca");
test_connect_ok($common_connstr,
	"sslrootcert=ssl/both-cas-2.crt sslmode=verify-ca");

note "testing sslcrl option with a non-revoked cert";

if ($supports_crl_files)
{
	# Invalid CRL filename is the same as no CRL, succeeds
	test_connect_ok($common_connstr,
		"sslrootcert=ssl/root+server_ca.crt sslmode=verify-ca sslcrl=invalid");

	# With the correct CRL, succeeds (this cert is not revoked)
	test_connect_ok($common_connstr,
	"sslrootcert=ssl/root+server_ca.crt sslmode=verify-ca sslcrl=ssl/root+server.crl"
	);

	# A CRL belonging to a different CA is not accepted, fails
	test_connect_fails($common_connstr,
	"sslrootcert=ssl/root+server_ca.crt sslmode=verify-ca sslcrl=ssl/client.crl");
}
else
{
	# Invalid CRL filename is the same as no CRL, succeeds
	test_connect_fails($common_connstr,
		"sslrootcert=ssl/root+server_ca.crt sslmode=verify-ca sslcrl=invalid");

	# With the correct CRL, succeeds (this cert is not revoked)
	test_connect_fails($common_connstr,
	"sslrootcert=ssl/root+server_ca.crt sslmode=verify-ca sslcrl=ssl/root+server.crl"
	);

	# A CRL belonging to a different CA is not accepted, this should fail iff
	# CRL files are supported
	test_connect_fails($common_connstr,
	"sslrootcert=ssl/root+server_ca.crt sslmode=verify-ca sslcrl=ssl/client.crl");
}

# Check that connecting with verify-full fails, when the hostname doesn't
# match the hostname in the server's certificate.
note "test mismatch between hostname and server certificate";
$common_connstr =
"user=ssltestuser dbname=trustdb sslcert=invalid sslrootcert=ssl/root+server_ca.crt hostaddr=$SERVERHOSTADDR sslmode=verify-full";

test_connect_ok($common_connstr, "sslmode=require host=wronghost.test");
test_connect_ok($common_connstr, "sslmode=verify-ca host=wronghost.test");
test_connect_fails($common_connstr, "sslmode=verify-full host=wronghost.test");

# Test Subject Alternative Names.
switch_server_cert($node, 'server-multiple-alt-names');

note "test hostname matching with X.509 Subject Alternative Names";
$common_connstr =
"user=ssltestuser dbname=trustdb sslcert=invalid sslrootcert=ssl/root+server_ca.crt hostaddr=$SERVERHOSTADDR sslmode=verify-full";

test_connect_ok($common_connstr, "host=dns1.alt-name.pg-ssltest.test");
test_connect_ok($common_connstr, "host=dns2.alt-name.pg-ssltest.test");
test_connect_ok($common_connstr, "host=foo.wildcard.pg-ssltest.test");

test_connect_fails($common_connstr, "host=wronghost.alt-name.pg-ssltest.test");
test_connect_fails($common_connstr,
	"host=deep.subdomain.wildcard.pg-ssltest.test");

# Test certificate with a single Subject Alternative Name. (this gives a
# slightly different error message, that's all)
switch_server_cert($node, 'server-single-alt-name');

note "test hostname matching with a single X.509 Subject Alternative Name";
$common_connstr =
"user=ssltestuser dbname=trustdb sslcert=invalid sslrootcert=ssl/root+server_ca.crt hostaddr=$SERVERHOSTADDR sslmode=verify-full";

test_connect_ok($common_connstr, "host=single.alt-name.pg-ssltest.test");

test_connect_fails($common_connstr, "host=wronghost.alt-name.pg-ssltest.test");
test_connect_fails($common_connstr,
	"host=deep.subdomain.wildcard.pg-ssltest.test");

# Test server certificate with a CN and SANs. Per RFCs 2818 and 6125, the CN
# should be ignored when the certificate has both.
switch_server_cert($node, 'server-cn-and-alt-names');

note "test certificate with both a CN and SANs";
$common_connstr =
"user=ssltestuser dbname=trustdb sslcert=invalid sslrootcert=ssl/root+server_ca.crt hostaddr=$SERVERHOSTADDR sslmode=verify-full";

test_connect_ok($common_connstr, "host=dns1.alt-name.pg-ssltest.test");
test_connect_ok($common_connstr, "host=dns2.alt-name.pg-ssltest.test");
test_connect_fails($common_connstr, "host=common-name.pg-ssltest.test");

# Finally, test a server certificate that has no CN or SANs. Of course, that's
# not a very sensible certificate, but libpq should handle it gracefully.
switch_server_cert($node, 'server-no-names');
$common_connstr =
"user=ssltestuser dbname=trustdb sslcert=invalid sslrootcert=ssl/root+server_ca.crt hostaddr=$SERVERHOSTADDR";

test_connect_ok($common_connstr,
	"sslmode=verify-ca host=common-name.pg-ssltest.test");
test_connect_fails($common_connstr,
	"sslmode=verify-full host=common-name.pg-ssltest.test");

# Test that the CRL works
note "testing client-side CRL";
switch_server_cert($node, 'server-revoked');

$common_connstr =
"user=ssltestuser dbname=trustdb sslcert=invalid hostaddr=$SERVERHOSTADDR host=common-name.pg-ssltest.test";

# Without the CRL, succeeds. With it, fails.
test_connect_ok($common_connstr,
	"sslrootcert=ssl/root+server_ca.crt sslmode=verify-ca");
if ($supports_crl_files)
{
	test_connect_fails($common_connstr,
	"sslrootcert=ssl/root+server_ca.crt sslmode=verify-ca sslcrl=ssl/root+server.crl"
	);
}
else
{
	test_connect_fails($common_connstr,
	"sslrootcert=ssl/root+server_ca.crt sslmode=verify-ca sslcrl=ssl/root+server.crl"
	);
}

### Part 2. Server-side tests.
###
### Test certificate authorization.

note "testing certificate authorization";
$common_connstr =
"sslrootcert=ssl/root+server_ca.crt sslmode=require dbname=certdb hostaddr=$SERVERHOSTADDR";

# no client cert
test_connect_fails($common_connstr, "user=ssltestuser sslcert=invalid");

# correct client cert
test_connect_ok($common_connstr,
	"user=ssltestuser sslcert=ssl/client.crt sslkey=ssl/client_tmp.key");

# client cert belonging to another user
test_connect_fails($common_connstr,
	"user=anotheruser sslcert=ssl/client.crt sslkey=ssl/client_tmp.key");

if ($supports_crl_files)
{
	# revoked client cert
	test_connect_fails($common_connstr,
	"user=ssltestuser sslcert=ssl/client-revoked.crt sslkey=ssl/client-revoked.key"
	);
}
else
{
	# revoked client cert
	test_connect_ok($common_connstr,
	"user=ssltestuser sslcert=ssl/client-revoked.crt sslkey=ssl/client-revoked.key"
	);
}

# intermediate client_ca.crt is provided by client, and isn't in server's ssl_ca_file
switch_server_cert($node, 'server-cn-only', 'root_ca');
$common_connstr =
"user=ssltestuser dbname=certdb sslkey=ssl/client_tmp.key sslrootcert=ssl/root+server_ca.crt hostaddr=$SERVERHOSTADDR";

test_connect_ok($common_connstr,
	"sslmode=require sslcert=ssl/client+client_ca.crt");
test_connect_fails($common_connstr, "sslmode=require sslcert=ssl/client.crt");

if ($supports_keychains)
{
	# empty keychain
	test_connect_fails("user=ssltestuser keychain=invalid");

	# correct client cert in keychain with and without proper label
	test_connect_fails("user=ssltestuser keychain=ssl/client.keychain");
	test_connect_ok("user=ssltestuser sslcert=ssltestuser keychain=ssl/client.keychain");
}

# clean up
unlink "ssl/client_tmp.key";
