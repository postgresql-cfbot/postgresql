# Check if backend stopped after client disconnection
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;
use File::Copy;

if ($ENV{with_openssl} eq 'yes')
{
    plan tests => 3;
}
else
{
    plan tests => 2;
}

my $long_query = q{
DO
$$
DECLARE row_data RECORD;
BEGIN
EXECUTE 'CREATE TABLE IF NOT EXISTS keep_alive_test AS SELECT generate_series(0,100000) AS tt';
FOR row_data IN
    SELECT tt
    FROM keep_alive_test
LOOP
    EXECUTE 'SELECT count(*) FROM keep_alive_test';
END LOOP;
END$$;
};
my $set_guc_on = q{
    SET client_connection_check_interval = 1000;
};
my $set_guc_off = q{
    SET client_connection_check_interval = 0;
};
my ($pid, $timed_out);

my $node = get_new_node('node');
$node->init;
$node->start;

#########################################################
# TEST 1: GUC client_connection_check_interval: enabled #
#########################################################

# Set GUC options, get backend pid and run a long time query
$node->psql('postgres', "$set_guc_on SELECT pg_backend_pid(); $long_query",
            stdout => \$pid, timeout => 2, timed_out => \$timed_out);

# Give time to the backend to detect client disconnected
sleep 3;
# Check if backend is still alive
my $is_alive = $node->safe_psql('postgres', "SELECT count(*) FROM pg_stat_activity where pid = $pid;");
is($is_alive, '0', 'Test: client_connection_check_interval enable');
$node->stop;

##########################################################
# TEST 2: GUC client_connection_check_interval: disabled #
##########################################################

$node->start;
$node->psql('postgres', "$set_guc_off SELECT pg_backend_pid(); $long_query",
            stdout => \$pid, timeout => 2, timed_out => \$timed_out);
# Give time to the client to disconnect
sleep 3;
# Check if backend is still alive
$is_alive = $node->safe_psql('postgres', "SELECT count(*) FROM pg_stat_activity where pid = $pid;");
is($is_alive, '1', 'Test: client_connection_check_interval disable');
$node->stop;

##########################################################
# TEST 3: Using client_connection_check_interval when    #
#         client connected using SSL                     #
##########################################################

if ($ENV{with_openssl} eq 'yes')
{
    # The client's private key must not be world-readable, so take a copy
    # of the key stored in the code tree and update its permissions.
    copy("../../ssl/ssl/client.key", "../../ssl/ssl/client_tmp.key");
    chmod 0600, "../../ssl/ssl/client_tmp.key";
    copy("../../ssl/ssl/client-revoked.key", "../../ssl/ssl/client-revoked_tmp.key");
    chmod 0600, "../../ssl/ssl/client-revoked_tmp.key";
    $ENV{PGHOST} = $node->host;
    $ENV{PGPORT} = $node->port;

    open my $sslconf, '>', $node->data_dir . "/sslconfig.conf";
    print $sslconf "ssl=on\n";
    print $sslconf "ssl_cert_file='server-cn-only.crt'\n";
    print $sslconf "ssl_key_file='server-password.key'\n";
    print $sslconf "ssl_passphrase_command='echo secret1'\n";
    close $sslconf;

    $node->start;
    $node->psql('postgres', "$set_guc_on SELECT pg_backend_pid(); $long_query",
                stdout => \$pid, timeout => 2, timed_out => \$timed_out,
                sslmode => 'require');

    # Give time to the backend to detect client disconnected
    sleep 3;
    # Check if backend is still alive
    my $is_alive = $node->safe_psql('postgres', "SELECT count(*) FROM pg_stat_activity where pid = $pid;");
    is($is_alive, '0', 'Test: client_connection_check_interval enabled, SSL');
    $node->stop;
}
