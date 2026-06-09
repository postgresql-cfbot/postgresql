use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use PgHttpService::Server;

if ($ENV{with_libcurl} ne 'yes')
{
	plan skip_all => 'HTTP service file not supported by this build';
}

if ($ENV{with_python} ne 'yes')
{
	plan skip_all => 'HTTP service tests require --with-python to run';
}

my $td = PostgreSQL::Test::Utils::tempdir;

my $node_dummy = PostgreSQL::Test::Cluster->new('node_dummy');
$node_dummy->init;

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

# Windows vs non-Windows: CRLF vs LF for the file's newline, relying on
# the fact that libpq uses fgets() when reading the lines of a service file.
my $newline = "\n";

# Create the set of service files used in the tests.
# File that includes a valid service name, that uses a decomposed connection
# string for its contents, split on spaces.
my $remote_srvfile_valid = "$td/remote_pg_service_valid.conf";
append_to_file($remote_srvfile_valid, qq{
port=} . $node->port() . qq{
host=} . $node->host() . qq{
dbname=postgres
});

my $missing_equals_invalid = "$td/missing_equals_invalid.conf";
append_to_file($missing_equals_invalid, qq{
port=} . $node->port() . qq{
host=} . $node->host() . qq{
dbname=
});

my $remote_pg_service_nonexistant_connection_option = "$td/remote_pg_service_nonexistant_connection_option.conf";
append_to_file($remote_pg_service_nonexistant_connection_option, qq{
port=} . $node->port() . qq{
nonexistant_param=something
dbname=postgres
});

my $remote_pg_service_ldap_connection_option = "$td/remote_pg_service_ldap_connection_option.conf";
append_to_file($remote_pg_service_ldap_connection_option, qq{
ldap://
});

my $remote_pg_service_http_connection_option = "$td/remote_pg_service_http_connection_option.conf";
append_to_file($remote_pg_service_http_connection_option, qq{
http://
});

my $remote_pg_service_nested_connection_option = "$td/remote_pg_service_nested_connection_option.conf";
append_to_file($remote_pg_service_nested_connection_option, qq{
service=nonexistent
});

# File defined with no contents, used as default value for PGSERVICEFILE,
# so as no lookup is attempted in the user's home directory.
my $srvfile_empty = "$td/pg_service_empty.conf";
append_to_file($srvfile_empty, '');

my $server = PgHttpService::Server->new();
$server->run($td);

my $port = $server->port;
my $issuer = "http://127.0.0.1:$port";

my $local_srvfile_valid = "$td/local_pg_service_valid.conf";
append_to_file($local_srvfile_valid, qq{
[http_service]
} . $issuer . qq{/remote_pg_service_valid.conf

[http_service_incorrect_scheme_with_fallback]
http:/127.0.0.1:} . $port . qq{
} . $issuer . qq{/remote_pg_service_valid.conf

[http_service_incorrect_scheme]
http:/127.0.0.1:} . $port . qq{/remote_pg_service_valid.conf

[http_service_non_listening_port]
http://127.0.0.1:1234/remote_pg_service_valid.conf

[http_service_non_listening_port_with_fallback]
http://127.0.0.1:1234
} . $issuer . qq{/remote_pg_service_valid.conf

[remote_pg_service_nonexistant_connection_option]
} . $issuer . qq{/remote_pg_service_nonexistant_connection_option.conf

[nested_http_service]
} . $issuer . qq{/remote_pg_service_http_connection_option.conf

[ldap_service]
} . $issuer . qq{/remote_pg_service_ldap_connection_option.conf

[remote_nested]
} . $issuer . qq{/remote_pg_service_nested_connection_option.conf

[missing_equals_invalid]
} . $issuer . qq{/missing_equals_invalid.conf
});

# Set the fallback directory lookup of the service file to the temporary
# directory of this test.  PGSYSCONFDIR is used if the service file
# defined in PGSERVICEFILE cannot be found, or when a service file is
# found but not the service name.
local $ENV{PGSYSCONFDIR} = $td;

# Force PGSERVICEFILE to a default location, so as this test never
# tries to look at a home directory.  This value needs to remain
# at the top of this script before running any tests, and should never
# be changed.

{
	local $ENV{PGSERVICEFILE} = $local_srvfile_valid;

	$node_dummy->connect_ok(
		'service=http_service',
		'connection with correct "service" string and PGSERVICEFILE',
		sql => "SELECT 'connect1_1'",
		expected_stdout => qr/connect1_1/);

	$node_dummy->connect_ok(
		'postgres://?service=http_service',
		'connection with correct "service" URI and PGSERVICEFILE',
		sql => "SELECT 'connect1_2'",
		expected_stdout => qr/connect1_2/);

	$node_dummy->connect_fails(
		'service=http_service_incorrect_scheme',
		'service with invalid http scheme correctly raises error',
		expected_stderr=> qr/psql: error: invalid HTTP URL \".*\": scheme must be http:\/\//);

	$node_dummy->connect_fails(
		'service=http_service_non_listening_port',
		'query on non listening port continues to use connection parameters from node_dummy',
		expected_stderr=>
		  qr/psql: error: connection to server on socket \"\/tmp\/.*\" failed: No such file or directory/);

	$node_dummy->connect_ok(
		'service=http_service_non_listening_port_with_fallback',
		'check that non listening URL allows fallback to listening URL',
		sql => "SELECT 'connect1_3'",
		expected_stdout => qr/connect1_3/);

	$node_dummy->connect_fails(
		'service=http_service_incorrect_scheme_with_fallback',
		'check that incorrect http address does not allow fallback',
		expected_stderr => qr/psql: error: invalid HTTP URL \".*\": scheme must be http:\/\//);

	$node_dummy->connect_fails(
		'service=remote_pg_service_nonexistant_connection_option',
		'',
		expected_stderr => qr/psql: error: syntax error in service file \"http:\/\/127\.0\.0\.1:.*\/remote_pg_service_nonexistant_connection_option.conf\"/);

	$node_dummy->connect_fails(
		'service=ldap_service',
		'check that ldap:// lines are not allowed in http service file',
		expected_stderr => qr/psql: error: ldap:\/\/ lines are not allowed in http service lookups/);

	$node_dummy->connect_fails(
		'service=nested_http_service',
		'check that http:// lines are not allowed in http service file',
		expected_stderr => qr/psql: error: http:\/\/ lines are not allowed in http service lookups/);

	$node_dummy->connect_fails(
		'service=remote_nested',
		'',
		expected_stdout => qr/psql: error: nested service specifications not supported in service file \"http:\/\/127\.0\.0\.1:.*\/remote_pg_service_nested_connection_option.conf\"/);

	$node_dummy->connect_fails(
		'service=missing_equals_invalid',
		'',
		expected_stdout => qr/psql: error: syntax error in service file \"http:\/\/127\.0\.0\.1:.*\/missing_equals_invalid.conf\"/);

}

$server->stop;

$node->teardown_node;

done_testing();
