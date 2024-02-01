use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use File::Copy;

use FindBin;
use lib $FindBin::RealBin;

use SSL::Server;

#### Some configuration

# This is the hostname used to connect to the server. This cannot be a
# hostname, because the server certificate is always for the domain
# postgresql-ssl-regression.test.
my $SERVERHOSTADDR = '127.0.0.1';
# This is the pattern to use in pg_hba.conf to match incoming connections.
my $SERVERHOSTCIDR = '127.0.0.1/32';

my $common_connstr;

sub connect_fails
{
	my ($node, $connstr, $test_name, %params) = @_;

	my $cmd = [
		'psql', '-X', '-A', '-t', '-c', "SELECT 'connected with $connstr'",
		'-d', "$common_connstr $connstr" ];

	my ($stdout, $stderr);

	print("# Running: " . join(" ", @{$cmd}) . "\n");
	my $result = IPC::Run::run $cmd, '>', \$stdout, '2>', \$stderr;

	if (defined($params{expected_stderr}))
	{
		like($stderr, $params{expected_stderr}, "$test_name: matches");
	}
}

my $ssl_server = SSL::Server->new();

# The client's private key must not be world-readable, so take a copy
# of the key stored in the code tree and update its permissions.
copy("ssl/client.key", "ssl/client_tmp.key");
chmod 0600, "ssl/client_tmp.key";

#### Part 0. Set up the server.

note "setting up data directory";
my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;

# PGHOST is enforced here to set up the node, subsequent connections
# will use a dedicated connection string.
$ENV{PGHOST} = $node->host;
$ENV{PGPORT} = $node->port;
$node->start;

$ssl_server->configure_test_server_for_ssl($node, $SERVERHOSTADDR,
    $SERVERHOSTCIDR, 'trust');

$ssl_server->switch_server_cert(
    $node,
    certfile => 'server-cn-only',
    crldir => 'root+client-crldir');

### Part 1. Run client-side tests.
###
### Test that libpq accepts/rejects the connection correctly, depending
### on sslmode and whether the server's certificate looks correct. No
### client certificate is used in these tests.

note "running client tests";

$common_connstr =
"user=ssltestuser dbname=trustdb sslcert=invalid hostaddr=$SERVERHOSTADDR host=common-name.pg-ssltest.test";


for (my $i = 1; $i <= 1000; $i++) {
print("iteration $i\n");
$node->connect_fails(
	"$common_connstr user=ssltestuser sslcert=ssl/client-revoked.crt "
	. $ssl_server->sslkey('client-revoked.key'),
	"certificate authorization fails with revoked client cert with server-side CRL directory",
	expected_stderr => qr/SSL error: sslv3 alert certificate revoked/,
	# temporarily(?) skip this check due to timing issue
	#   log_like => [
	#       qr{Client certificate verification failed at depth 0: certificate revoked},
	#       qr{Failed certificate data \(unverified\): subject "/CN=ssltestuser", serial number 2315134995201656577, issuer "/CN=Test CA for PostgreSQL SSL regression test client certs"},
	#   ]
);
}

# clean up
unlink "ssl/client_tmp.key";

done_testing();
