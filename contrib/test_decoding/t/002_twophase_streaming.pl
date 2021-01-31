# logical replication of 2PC test
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 2;
use Time::HiRes qw(usleep);
use Scalar::Util qw(looks_like_number);

# Initialize node
my $node_logical = get_new_node('logical');
$node_logical->init(allows_streaming => 'logical');
$node_logical->append_conf(
        'postgresql.conf', qq(
        max_prepared_transactions = 10
		logical_decoding_work_mem = 64kB
));
$node_logical->start;

# Create some pre-existing content on logical
$node_logical->safe_psql('postgres', "CREATE TABLE stream_test (data text)");
$node_logical->safe_psql('postgres',
	"INSERT INTO stream_test SELECT repeat('a', 10) || g.i FROM generate_series(1,3) g(i)");
$node_logical->safe_psql('postgres',
	"SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding');");

#Test 1:
# This test is specifically for testing concurrent abort while logical decode
# is ongoing. We will pass in the xid of the 2PC to the plugin as an option.
# On the receipt of a valid "check-xid-aborted", the change API in the test decoding
# plugin will wait for it to be aborted.
#
# We will fire off a ROLLBACK from another session when this decode
# is waiting.
#
# The status of "check-xid-aborted" will change from in-progress to not-committed
# (hence aborted) and we will stop decoding because the subsequent
# system catalog scan will error out.

$node_logical->safe_psql('postgres', "
    BEGIN;
	savepoint s1;
	SELECT 'msg5' FROM pg_logical_emit_message(true, 'test', repeat('a', 50));
	INSERT INTO stream_test SELECT repeat('a', 2000) || g.i FROM generate_series(1, 35) g(i);
	TRUNCATE table stream_test;
	rollback to s1;
	INSERT INTO stream_test SELECT repeat('a', 10) || g.i FROM generate_series(1, 20) g(i);
	PREPARE TRANSACTION 'test1';");
# get XID of the above two-phase transaction
my $xid2pc = $node_logical->safe_psql('postgres', "SELECT transaction FROM pg_prepared_xacts WHERE gid = 'test1'");
is(looks_like_number($xid2pc), qq(1), 'Got a valid two-phase XID');

# start decoding the above by passing the "check-xid-aborted"
my $logical_connstr = $node_logical->connstr . ' dbname=postgres';

# decode now, it should include an ABORT entry because of the ROLLBACK below
system_log("psql -d \"$logical_connstr\" -c \"SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'two-phase-commit', '1', 'include-xids', '0', 'skip-empty-xacts', '1', 'check-xid-aborted', '$xid2pc','stream-changes', '1');\" \&");

# check that decode starts waiting for this $xid2pc
poll_output_until("waiting for $xid2pc to abort")
    or die "no wait happened for the abort";

# rollback the prepared transaction
$node_logical->safe_psql('postgres', "ROLLBACK PREPARED 'test1';");

# check for occurrence of the log about stopping this decoding
poll_output_until("stop decoding of txn $xid2pc")
    or die "no decoding stop for the rollback";

# consume any remaining changes
$node_logical->safe_psql('postgres', "SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'two-phase-commit', '1', 'include-xids', '0', 'skip-empty-xacts', '1','stream-changes', '1');");

# Test 2:
# Check concurrent aborts while decoding a TRUNCATE.

$node_logical->safe_psql('postgres', "
    BEGIN;
    savepoint s1;
    SELECT 'msg5' FROM pg_logical_emit_message(true, 'test', repeat('a', 50));
    INSERT INTO stream_test SELECT repeat('a', 2000) || g.i FROM generate_series(1, 35) g(i);
    TRUNCATE table stream_test;
    rollback to s1;
    TRUNCATE table stream_test;
    PREPARE TRANSACTION 'test2';");
# get XID of the above two-phase transaction
$xid2pc = $node_logical->safe_psql('postgres', "SELECT transaction FROM pg_prepared_xacts WHERE gid = 'test2'");
is(looks_like_number($xid2pc), qq(1), 'Got a valid two-phase XID');

# decode now, it should include an ABORT entry because of the ROLLBACK below
system_log("psql -d \"$logical_connstr\" -c \"SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'two-phase-commit', '1', 'include-xids', '0', 'skip-empty-xacts', '1', 'check-xid-aborted', '$xid2pc','stream-changes', '1');\" \&");

# check that decode starts waiting for this $xid2pc
poll_output_until("waiting for $xid2pc to abort")
    or die "no wait happened for the abort";

# rollback the prepared transaction
$node_logical->safe_psql('postgres', "ROLLBACK PREPARED 'test2';");

# check for occurrence of the log about stopping this decoding
poll_output_until("stop decoding of txn $xid2pc")
    or die "no decoding stop for the rollback";


$node_logical->safe_psql('postgres', "SELECT pg_drop_replication_slot('regression_slot');");
$node_logical->stop('fast');

sub poll_output_until
{
    my ($expected) = @_;

    $expected = 'xxxxxx' unless defined($expected); # default junk value

    my $max_attempts = 180 * 10;
    my $attempts     = 0;

    my $output_file = '';
    while ($attempts < $max_attempts)
    {
        $output_file = slurp_file($node_logical->logfile());

        if ($output_file =~ $expected)
        {
            return 1;
        }

        # Wait 0.1 second before retrying.
        usleep(100_000);
        $attempts++;
    }

    # The output result didn't change in 180 seconds. Give up
    return 0;
}
