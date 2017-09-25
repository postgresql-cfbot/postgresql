use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 8;
use ServerSetup;
use File::Copy;

# test combinations of SASL authentication for SCRAM mechanism:
# - SCRAM-SHA-256 and SCRAM-SHA-256-PLUS
# - Channel bindings

# This is the hostname used to connect to the server.
my $SERVERHOSTADDR = '127.0.0.1';

# Allocation of base connection string shared among multiple tests.
my $common_connstr;

#### Part 0. Set up the server.

note "setting up data directory";
my $node = get_new_node('master');
$node->init;

# PGHOST is enforced here to set up the node, subsequent connections
# will use a dedicated connection string.
$ENV{PGHOST} = $node->host;
$ENV{PGPORT} = $node->port;
$node->start;

# Configure server for SSL connections, with password handling.
configure_test_server_for_ssl($node, $SERVERHOSTADDR, "scram-sha-256",
							  "pass", "scram-sha-256");
switch_server_cert($node, 'server-cn-only');
$ENV{PGPASSWORD} = "pass";
$common_connstr =
"user=ssltestuser dbname=trustdb sslmode=require hostaddr=$SERVERHOSTADDR";

# Tests with default channel binding and SASL mechanism names.
# tls-unique is used here
test_connect_ok($common_connstr, "saslname=SCRAM-SHA-256-PLUS");
test_connect_fails($common_connstr, "saslname=not-exists");
# Downgrade attack.
test_connect_fails($common_connstr, "saslname=SCRAM-SHA-256");
test_connect_fails($common_connstr,
		"saslname=SCRAM-SHA-256 saslchannelbinding=tls-unique");
test_connect_fails($common_connstr,
		"saslname=SCRAM-SHA-256 saslchannelbinding=tls-server-end-point");

# Channel bindings
test_connect_ok($common_connstr,
		"saslname=SCRAM-SHA-256-PLUS saslchannelbinding=tls-unique");
test_connect_ok($common_connstr,
		"saslname=SCRAM-SHA-256-PLUS saslchannelbinding=tls-server-end-point");
test_connect_fails($common_connstr,
		"saslname=SCRAM-SHA-256-PLUS saslchannelbinding=not-exists");
