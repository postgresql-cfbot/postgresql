
# Copyright (c) 2021-2024, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use JSON::PP qw(encode_json);
use MIME::Base64 qw(encode_base64);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::OAuthServer;
use Test::More;

if ($ENV{with_oauth} ne 'curl')
{
	plan skip_all => 'client-side OAuth not supported by this build';
}
elsif ($ENV{with_python} ne 'yes')
{
	plan skip_all => 'OAuth tests require --with-python to run';
}

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;
$node->append_conf('postgresql.conf', "log_connections = on\n");
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'validator'\n");
$node->append_conf('postgresql.conf', "oauth_validator_library = 'validator'\n");
$node->start;

$node->safe_psql('postgres', 'CREATE USER test;');
$node->safe_psql('postgres', 'CREATE USER testalt;');
$node->safe_psql('postgres', 'CREATE USER testparam;');

# Save a background connection for later configuration changes.
my $bgconn = $node->background_psql('postgres');

my $webserver = PostgreSQL::Test::OAuthServer->new();
$webserver->run();

END
{
	my $exit_code = $?;

	$webserver->stop() if defined $webserver; # might have been SKIP'd

	$? = $exit_code;
}

my $port = $webserver->port();
my $issuer = "127.0.0.1:$port";

unlink($node->data_dir . '/pg_hba.conf');
$node->append_conf('pg_hba.conf', qq{
local all test      oauth issuer="$issuer"           scope="openid postgres"
local all testalt   oauth issuer="$issuer/alternate" scope="openid postgres alt"
local all testparam oauth issuer="$issuer/param"     scope="openid postgres"
});
$node->reload;

my ($log_start, $log_end);
$log_start = $node->wait_for_log(qr/reloading configuration files/);


# To test against HTTP rather than HTTPS, we need to enable PGOAUTHDEBUG. But
# first, check to make sure the client refuses such connections by default.
$node->connect_fails("user=test dbname=postgres oauth_client_id=f02c6361-0635",
					 "HTTPS is required without debug mode",
					 expected_stderr => qr/failed to fetch OpenID discovery document: Unsupported protocol/);

$ENV{PGOAUTHDEBUG} = "UNSAFE";

my $user = "test";
if ($node->connect_ok("user=$user dbname=postgres oauth_client_id=f02c6361-0635", "connect",
					  expected_stderr => qr@Visit https://example\.com/ and enter the code: postgresuser@))
{
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
}

# The /alternate issuer uses slightly different parameters.
$user = "testalt";
if ($node->connect_ok("user=$user dbname=postgres oauth_client_id=f02c6361-0636", "connect",
					  expected_stderr => qr@Visit https://example\.org/ and enter the code: postgresuser@))
{
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
}

#
# Further tests rely on support for specific behaviors in oauth_server.py. To
# trigger these behaviors, we ask for the special issuer .../param (which is set
# up in HBA for the testparam user) and encode magic instructions into the
# oauth_client_id.
#

my $common_connstr = "user=testparam dbname=postgres ";

sub connstr
{
	my (%params) = @_;

	my $json = encode_json(\%params);
	my $encoded = encode_base64($json, "");

	return "$common_connstr oauth_client_id=$encoded";
}

# Make sure the param system works end-to-end first.
$node->connect_ok(
	connstr(),
	"connect to /param",
	expected_stderr => qr@Visit https://example\.com/ and enter the code: postgresuser@
);

$node->connect_ok(
	connstr(stage => 'token', retries => 1),
	"token retry",
	expected_stderr => qr@Visit https://example\.com/ and enter the code: postgresuser@
);
$node->connect_ok(
	connstr(stage => 'token', retries => 2),
	"token retry (twice)",
	expected_stderr => qr@Visit https://example\.com/ and enter the code: postgresuser@
);
$node->connect_ok(
	connstr(stage => 'all', retries => 1, interval => 2),
	"token retry (two second interval)",
	expected_stderr => qr@Visit https://example\.com/ and enter the code: postgresuser@
);
$node->connect_ok(
	connstr(stage => 'all', retries => 1, interval => JSON::PP::null),
	"token retry (default interval)",
	expected_stderr => qr@Visit https://example\.com/ and enter the code: postgresuser@
);

$node->connect_ok(
	connstr(stage => 'all', content_type => 'application/json;charset=utf-8'),
	"content type with charset",
	expected_stderr => qr@Visit https://example\.com/ and enter the code: postgresuser@
);
$node->connect_ok(
	connstr(stage => 'all', content_type => "application/json \t;\t charset=utf-8"),
	"content type with charset (whitespace)",
	expected_stderr => qr@Visit https://example\.com/ and enter the code: postgresuser@
);
$node->connect_ok(
	connstr(stage => 'device', uri_spelling => "verification_url"),
	"alternative spelling of verification_uri",
	expected_stderr => qr@Visit https://example\.com/ and enter the code: postgresuser@
);

$node->connect_fails(
	connstr(stage => 'device', huge_response => JSON::PP::true),
	"bad device authz response: overlarge JSON",
	expected_stderr => qr/failed to obtain device authorization: response is too large/
);
$node->connect_fails(
	connstr(stage => 'token', huge_response => JSON::PP::true),
	"bad token response: overlarge JSON",
	expected_stderr => qr/failed to obtain access token: response is too large/
);

$node->connect_fails(
	connstr(stage => 'device', content_type => 'text/plain'),
	"bad device authz response: wrong content type",
	expected_stderr => qr/failed to parse device authorization: unexpected content type/
);
$node->connect_fails(
	connstr(stage => 'token', content_type => 'text/plain'),
	"bad token response: wrong content type",
	expected_stderr => qr/failed to parse access token response: unexpected content type/
);
$node->connect_fails(
	connstr(stage => 'token', content_type => 'application/jsonx'),
	"bad token response: wrong content type (correct prefix)",
	expected_stderr => qr/failed to parse access token response: unexpected content type/
);

$node->connect_fails(
	connstr(stage => 'all', interval => ~0, retries => 1, retry_code => "slow_down"),
	"bad token response: server overflows the device authz interval",
	expected_stderr => qr/failed to obtain access token: slow_down interval overflow/
);

$node->connect_fails(
	connstr(stage => 'token', error_code => "invalid_grant"),
	"bad token response: invalid_grant, no description",
	expected_stderr => qr/failed to obtain access token: \(invalid_grant\)/);
$node->connect_fails(
	connstr(
		stage => 'token',
		error_code => "invalid_grant",
		error_desc => "grant expired"),
	"bad token response: expired grant",
	expected_stderr =>
	  qr/failed to obtain access token: grant expired \(invalid_grant\)/);
$node->connect_fails(
	connstr(
		stage => 'token',
		error_code => "invalid_client",
		error_status => 401),
	"bad token response: client authentication failure, default description",
	expected_stderr =>
	  qr/failed to obtain access token: provider requires client authentication, and no oauth_client_secret is set \(invalid_client\)/
);
$node->connect_fails(
	connstr(
		stage => 'token',
		error_code => "invalid_client",
		error_status => 401,
		error_desc => "authn failure"),
	"bad token response: client authentication failure, provided description",
	expected_stderr =>
	  qr/failed to obtain access token: authn failure \(invalid_client\)/);

$node->connect_fails(
	connstr(stage => 'token', token => ""),
	"server rejects access: empty token",
	expected_stderr => qr/bearer authentication failed/);
$node->connect_fails(
	connstr(stage => 'token', token => "****"),
	"server rejects access: invalid token contents",
	expected_stderr => qr/bearer authentication failed/);

# Test behavior of the oauth_client_secret.
$common_connstr = "$common_connstr oauth_client_secret=12345";

$node->connect_ok(
	connstr(stage => 'all', expected_secret => '12345'),
	"oauth_client_secret",
	expected_stderr =>
	  qr@Visit https://example\.com/ and enter the code: postgresuser@);

$node->connect_fails(
	connstr(
		stage => 'token',
		error_code => "invalid_client",
		error_status => 401),
	"bad token response: client authentication failure, default description with oauth_client_secret",
	expected_stderr =>
	  qr/failed to obtain access token: provider rejected the oauth_client_secret \(invalid_client\)/
);
$node->connect_fails(
	connstr(
		stage => 'token',
		error_code => "invalid_client",
		error_status => 401,
		error_desc => "mutual TLS required for client"),
	"bad token response: client authentication failure, provided description with oauth_client_secret",
	expected_stderr =>
	  qr/failed to obtain access token: mutual TLS required for client \(invalid_client\)/
);

#
# This section of tests reconfigures the validator module itself, rather than
# the OAuth server.
#

# Searching the logs is easier if OAuth parameter discovery isn't cluttering
# things up; hardcode the issuer.
$common_connstr = "user=test dbname=postgres oauth_issuer=$issuer";

$bgconn->query_safe("ALTER SYSTEM SET oauth_validator.authn_id TO ''");
$node->reload;
$log_start =
  $node->wait_for_log(qr/reloading configuration files/, $log_start);

if ($node->connect_fails(
		"$common_connstr oauth_client_id=f02c6361-0635",
		"validator must set authn_id",
		expected_stderr => qr/OAuth bearer authentication failed/))
{
	$log_end =
	  $node->wait_for_log(qr/FATAL:\s+OAuth bearer authentication failed/,
		$log_start);

	$node->log_check(
		"validator must set authn_id: breadcrumbs are logged",
		$log_start,
		log_like => [
			qr/connection authenticated: identity=""/,
			qr/DETAIL:\s+Validator provided no identity/,
			qr/FATAL:\s+OAuth bearer authentication failed/,
		]);

	$log_start = $log_end;
}

$bgconn->query_safe("ALTER SYSTEM RESET oauth_validator.authn_id");
$node->reload;
$log_start =
  $node->wait_for_log(qr/reloading configuration files/, $log_start);

$node->stop;

done_testing();
