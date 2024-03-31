
# Copyright (c) 2021-2024, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::OAuthServer;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;
$node->append_conf('postgresql.conf', "log_connections = on\n");
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'validator'\n");
$node->append_conf('postgresql.conf', "oauth_validator_library = 'validator'\n");
$node->start;

$node->safe_psql('postgres', 'CREATE USER test;');
$node->safe_psql('postgres', 'CREATE USER testalt;');

my $webserver = PostgreSQL::Test::OAuthServer->new();
$webserver->run();

my $port = $webserver->port();
my $issuer = "127.0.0.1:$port";

unlink($node->data_dir . '/pg_hba.conf');
$node->append_conf('pg_hba.conf', qq{
local all test    oauth issuer="$issuer"           scope="openid postgres"
local all testalt oauth issuer="$issuer/alternate" scope="openid postgres alt"
});
$node->reload;

my ($log_start, $log_end);
$log_start = $node->wait_for_log(qr/reloading configuration files/);

my $user = "test";
$node->connect_ok("user=$user dbname=postgres oauth_client_id=f02c6361-0635", "connect",
				  expected_stderr => qr@Visit https://example\.com/ and enter the code: postgresuser@);

$log_end = $node->wait_for_log(qr/connection authorized/, $log_start);
$node->log_check("user $user: validator receives correct parameters", $log_start,
				 log_like => [
					 qr/oauth_validator: token="9243959234", role="$user"/,
					 qr/oauth_validator: issuer="\Q$issuer\E", scope="openid postgres"/,
				 ]);
$node->log_check("user $user: validator sets authenticated identity", $log_start,
				 log_like => [
					 qr/connection authenticated: identity="test" method=oauth/,
				 ]);
$log_start = $log_end;

# The /alternate issuer uses slightly different parameters.
$user = "testalt";
$node->connect_ok("user=$user dbname=postgres oauth_client_id=f02c6361-0636", "connect",
				  expected_stderr => qr@Visit https://example\.org/ and enter the code: postgresuser@);

$log_end = $node->wait_for_log(qr/connection authorized/, $log_start);
$node->log_check("user $user: validator receives correct parameters", $log_start,
				 log_like => [
					 qr/oauth_validator: token="9243959234-alt", role="$user"/,
					 qr|oauth_validator: issuer="\Q$issuer/alternate\E", scope="openid postgres alt"|,
				 ]);
$node->log_check("user $user: validator sets authenticated identity", $log_start,
				 log_like => [
					 qr/connection authenticated: identity="testalt" method=oauth/,
				 ]);
$log_start = $log_end;

$webserver->stop();
$node->stop;

done_testing();
