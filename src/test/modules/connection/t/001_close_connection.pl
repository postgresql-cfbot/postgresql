# Check if backend stopped after client disconnection
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 2;

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
my $set_guc = q{
    SET client_connection_check_interval = 1000;
};

my $node = get_new_node('node');
my ($pid, $timed_out);
$node->init;
$node->start;

#########################################################
# TEST 1: GUC client_connection_check_interval: enabled #
#########################################################

# Set GUC options, get backend pid and run a long time query
$node->psql('postgres', "$set_guc SELECT pg_backend_pid(); $long_query",
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
$set_guc = q{
    SET client_connection_check_interval = 0;
};
$node->psql('postgres', "$set_guc SELECT pg_backend_pid(); $long_query",
            stdout => \$pid, timeout => 2, timed_out => \$timed_out);
# Give time to the client to disconnect
sleep 3;
# Check if backend is still alive
$is_alive = $node->safe_psql('postgres', "SELECT count(*) FROM pg_stat_activity where pid = $pid;");
is($is_alive, '1', 'Test: client_connection_check_interval disable');
$node->stop;
