
# Copyright (c) 2024, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use FindBin;
use lib $FindBin::RealBin;

use SSL::Server;

# This is the hostname used to connect to the server. This cannot be a
# hostname, because the server certificate is always for the domain
# postgresql-ssl-regression.test.
my $SERVERHOSTADDR = '127.0.0.1';
# This is the pattern to use in pg_hba.conf to match incoming connections.
my $SERVERHOSTCIDR = '127.0.0.1/32';

if ($ENV{with_ssl} ne 'openssl')
{
	plan skip_all => 'OpenSSL not supported by this build';
}

if (!$ENV{PG_TEST_EXTRA} || $ENV{PG_TEST_EXTRA} !~ /\bssl\b/)
{
	plan skip_all =>
	  'Potentially unsafe test SSL not enabled in PG_TEST_EXTRA';
}

sub reset_pg_hosts
{
	my $node = shift;

	ok(unlink($node->data_dir . '/pg_hosts.conf'));
	$node->append_conf('pg_hosts.conf', "localhost server.crt server.key root.crt");
	$node->reload;
	return;
}

my $ssl_server = SSL::Server->new();

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;

# PGHOST is enforced here to set up the node, subsequent connections
# will use a dedicated connection string.
$ENV{PGHOST} = $node->host;
$ENV{PGPORT} = $node->port;
$node->start;

$ssl_server->configure_test_server_for_ssl($node, $SERVERHOSTADDR,
	$SERVERHOSTCIDR, 'trust');

$ssl_server->switch_server_cert($node, certfile => 'server-cn-only');

my $connstr =
  "dbname=trustdb hostaddr=$SERVERHOSTADDR host=localhost sslsni=1";

$node->append_conf('postgresql.conf', "ssl_snimode=default");
$node->reload;

$node->connect_ok(
	"$connstr sslrootcert=ssl/root+server_ca.crt sslmode=require",
	"connect with correct server CA cert file sslmode=require");

$node->append_conf('postgresql.conf', "ssl_snimode=strict");
$node->reload;

$node->connect_fails(
	"$connstr sslrootcert=ssl/root+server_ca.crt sslmode=require",
	"connect with correct server CA cert file sslmode=require",
	expected_stderr => qr/unrecognized name/);

ok(unlink($node->data_dir . '/pg_hosts.conf'));
$node->append_conf('pg_hosts.conf', "localhost server-cn-only.crt server-cn-only.key root_ca.crt");
$node->reload;

$node->connect_ok(
	"$connstr sslrootcert=ssl/root+server_ca.crt sslmode=require",
	"connect with correct server CA cert file sslmode=require");

done_testing();
