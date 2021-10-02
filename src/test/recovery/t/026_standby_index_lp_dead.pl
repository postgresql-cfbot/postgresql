# Checks that index hints on standby work as excepted.
use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 27;
use Config;

# Initialize primary node
my $node_primary = PostgresNode->new('primary');
$node_primary->init(allows_streaming => 1);
$node_primary->append_conf('postgresql.conf', qq{
    autovacuum = off
    enable_seqscan = off
    enable_indexonlyscan = off
});
$node_primary->start;

$node_primary->safe_psql('postgres', 'CREATE EXTENSION pageinspect');
# Create test table with primary index
$node_primary->safe_psql(
    'postgres', 'CREATE TABLE test_table (id int, value int)');
$node_primary->safe_psql(
    'postgres', 'CREATE INDEX test_index ON test_table (value, id)');
# Fill some data to it, note to not put a lot of records to avoid
# heap_page_prune_opt call which cause conflict on recovery hiding conflict
# caused due index hint bits
$node_primary->safe_psql('postgres',
    'INSERT INTO test_table VALUES (generate_series(1, 30), 0)');
# And vacuum to allow index hint bits to be set
$node_primary->safe_psql('postgres', 'VACUUM test_table');
# For fail-fast in case FPW from primary
$node_primary->safe_psql('postgres', 'CHECKPOINT');

# Take backup
my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

# Restore standby node from backup backup
my $node_standby_1 = PostgresNode->new('standby_1');
$node_standby_1->init_from_backup($node_primary, $backup_name,
    has_streaming => 1);

my $standby_settings = qq{
    max_standby_streaming_delay = 1
    wal_receiver_status_interval = 1
    hot_standby_feedback = off
    enable_seqscan = off
    enable_indexonlyscan = off
};
$node_standby_1->append_conf('postgresql.conf', $standby_settings);
$node_standby_1->start;

$node_standby_1->backup($backup_name);

# Create second standby node linking to standby 1
my $node_standby_2 = PostgresNode->new('standby_2');
$node_standby_2->init_from_backup($node_standby_1, $backup_name,
    has_streaming => 1);
$node_standby_2->append_conf('postgresql.conf', $standby_settings);
$node_standby_2->start;

# To avoid hanging while expecting some specific input from a psql
# instance being driven by us, add a timeout high enough that it
# should never trigger even on very slow machines, unless something
# is really wrong.
my $psql_timeout = IPC::Run::timer(10);

# One psql to run command in repeatable read isolation level
# It is used to test xactStartedInRecovery snapshot after promotion.
my %psql_standby_repeatable_read = ('stdin' => '', 'stdout' => '', 'stderr' => '');
$psql_standby_repeatable_read{run} =
    IPC::Run::start(
        [ 'psql', '-XAb', '-f', '-', '-d', $node_standby_1->connstr('postgres') ],
        '<', \$psql_standby_repeatable_read{stdin},
        '>', \$psql_standby_repeatable_read{stdout},
        '2>', \$psql_standby_repeatable_read{stderr},
        $psql_timeout);

# Another psql to run command in read committed isolation level
my %psql_standby_read_committed = ('stdin' => '', 'stdout' => '', 'stderr' => '');
$psql_standby_read_committed{run} =
    IPC::Run::start(
        [ 'psql', '-XAb', '-f', '-', '-d', $node_standby_1->connstr('postgres') ],
        '<', \$psql_standby_read_committed{stdin},
        '>', \$psql_standby_read_committed{stdout},
        '2>', \$psql_standby_read_committed{stderr},
        $psql_timeout);

# Start RR transaction and read first row from index
ok(send_query_and_wait(\%psql_standby_repeatable_read,
    q[
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SELECT id FROM test_table WHERE value = 0 ORDER BY id LIMIT 1;
],
    qr/1\n\(1 row\)/m),
    'row is visible in repeatable read');

# Start RC transaction and read first row from index
ok(send_query_and_wait(\%psql_standby_read_committed,
    q[
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
SELECT id FROM test_table WHERE value = 0 ORDER BY id LIMIT 1;
],
    qr/1\n\(1 row\)/m),
    'row is visible in read committed');

# Now delete first 10 rows in index
$node_primary->safe_psql('postgres',
    'UPDATE test_table SET value = 1 WHERE id <= 10');

# Make sure hint bits are not set on primary
is(hints_num($node_primary), qq(0), 'no index hint bits are set on primary yet');

# Make sure page is not processed by heap_page_prune_opt
is(non_normal_num($node_primary), qq(0), 'all items are normal in heap');

# Wait for standbys to catch up transaction
wait_for_catchup_all();

is(hints_num($node_standby_1), qq(0), 'no index hint bits are set on standby 1 yet');
is(hints_num($node_standby_2), qq(0), 'no index hint bits are set on standby 2 yet');

# Try to set hint bits in index on standbys
try_to_set_hint_bits($node_standby_1);
try_to_set_hint_bits($node_standby_2);

# Make sure previous queries not set the hints on standby because
# of RR snapshot
is(hints_num($node_standby_1), qq(0), 'no index hint bits are set on standby 1 yet');
is(btp_safe_on_stanby($node_standby_1), qq(0), 'hint are not marked as standby-safe');

# At the same time hint bits are set on second standby
is(hints_num($node_standby_2), qq(10), 'index hint bits already set on second standby 2');
is(btp_safe_on_stanby($node_standby_2), qq(1), 'hints are marked as standby-safe');

# Make sure read committed transaction is able to see correct data
ok(send_query_and_wait(\%psql_standby_read_committed,
    q/SELECT id FROM test_table WHERE value = 0 ORDER BY id LIMIT 1;/,
    qr/11\n\(1 row\)/m),
    'row is not visible in read committed');

ok(send_query_and_wait(\%psql_standby_repeatable_read,
    q/SELECT id FROM test_table WHERE value = 0 ORDER BY id LIMIT 1;/,
    qr/1\n\(1 row\)/m),
    'row is visible in repeatable read');

# Make checkpoint to cause FPI by LP_DEAD on primary
$node_primary->safe_psql('postgres', "CHECKPOINT");

# Set index hint bits and replicate to standby as FPI
$node_primary->safe_psql('postgres',
    'SELECT id FROM test_table WHERE value = 0 ORDER BY id LIMIT 1;');

# Make sure page is not processed by heap_page_prune_opt
is(non_normal_num($node_primary), qq(0), 'all items are normal in heap');
# Make sure hint bits are set
is(hints_num($node_primary), qq(10), 'hint bits are set on primary already');

## Wait for standbys to catch up hint bits
wait_for_catchup_all();

is(hints_num($node_standby_1), qq(10),
    'hints are set on standby1 because FPI but marked as non-safe');
is(btp_safe_on_stanby($node_standby_1), qq(0),
    'hint not marked as standby-safe');
is(hints_num($node_standby_2), qq(10),
    'hints are set on standby1 because FPI but masked as non-safe');
is(btp_safe_on_stanby($node_standby_2), qq(0),
    'hint not marked as standby-safe');

# Make sure read committed transaction is able to see correct data
ok(send_query_and_wait(\%psql_standby_read_committed,
    q/SELECT id FROM test_table WHERE value = 0 ORDER BY id LIMIT 1;/,
    qr/11\n\(1 row\)/m),
    'row is not visible in read committed');

# Make sure repeatable read transaction able to see correct data
# because hint bits are marked as non-safe
ok(send_query_and_wait(\%psql_standby_repeatable_read,
    q/SELECT id FROM test_table WHERE value = 0 ORDER BY id LIMIT 1;/,
    qr/1\n\(1 row\)/m),
    'row is visible in repeatable read');

$node_primary->stop();

# promote standby to new primary
$node_standby_1->promote();
my $node_new_primary = $node_standby_1;

# Make sure read committed transaction is able to see correct data
ok(send_query_and_wait(\%psql_standby_read_committed,
    q/SELECT id FROM test_table WHERE value = 0 ORDER BY id LIMIT 1;/,
    qr/11\n\(1 row\)/m),
    'row is not visible in read committed after promote');

# Make sure repeatable read transaction able to see correct data
# because hint bits are marked as non-safe and transaction was started on standby
ok(send_query_and_wait(\%psql_standby_repeatable_read,
    q/SELECT id FROM test_table WHERE value = 0 ORDER BY id LIMIT 1;/,
    qr/1\n\(1 row\)/m),
    'row is visible in repeatable read after promote');

$psql_standby_repeatable_read{run}->finish;
$psql_standby_read_committed{run}->finish;

# Remove one more row
$node_new_primary->safe_psql('postgres',
    'UPDATE test_table SET value = 1 WHERE id <= 11');

# Set one more index hint bit as on primary
$node_new_primary->safe_psql('postgres',
    'SELECT id FROM test_table WHERE value = 0 ORDER BY id LIMIT 1;');
is(hints_num($node_new_primary), qq(11), 'hint bits are set on new primary already');

my $new_backup_name = 'my_new_backup';
$node_new_primary->backup($new_backup_name);

# Create third standby node linking to promoted primary
my $node_new_standby = PostgresNode->new('standby_3');
$node_new_standby->init_from_backup($node_new_primary, $new_backup_name,
    has_streaming => 1);
$node_new_standby->append_conf('postgresql.conf', $standby_settings);
$node_new_standby->start;

is(hints_num($node_new_standby), qq(11), 'hint bits are from backup on new standby');
is(btp_safe_on_stanby($node_new_standby), qq(0), 'hint not marked as standby-safe');

# Remove one more row
$node_new_primary->safe_psql('postgres',
    'UPDATE test_table SET value = 1 WHERE id <= 12');

# Issue checkpoint on primary to update minRecoveryPoint on standby
$node_new_primary->safe_psql('postgres', "CHECKPOINT");

$node_new_primary->wait_for_catchup($node_new_standby, 'replay',
    $node_new_primary->lsn('insert'));

# Clear hint bits from base backup and set own
try_to_set_hint_bits($node_new_standby);
is(hints_num($node_new_standby), qq(12), 'hint bits are set on new standby');
is(btp_safe_on_stanby($node_new_standby), qq(1), 'hint now marked as standby-safe');

$node_new_primary->stop();
$node_standby_2->stop();
$node_new_standby->stop();

# Send query, wait until string matches
sub send_query_and_wait {
    my ($psql, $query, $untl) = @_;

    # send query
    $$psql{stdin} .= $query;
    $$psql{stdin} .= "\n";

    # wait for query results
    $$psql{run}->pump_nb();
    while (1) {
        # See PostgresNode.pm's psql()
        $$psql{stdout} =~ s/\r\n/\n/g if $Config{osname} eq 'msys';

        # diag("\n" . $$psql{stdout}); # for debugging
        # diag("\n" . $$psql{stderr}); # for debugging

        last if $$psql{stdout} =~ /$untl/;
        last if $$psql{stderr} =~ /$untl/;

        if ($psql_timeout->is_expired) {
            BAIL_OUT("aborting wait: program timed out \n" .
                "stream contents: >>$$psql{stdout}<< \n" .
                "pattern searched for: $untl");
            return 0;
        }
        if (not $$psql{run}->pumpable()) {
            # This is fine for some tests, keep running
            return 0;
        }
        $$psql{run}->pump();
        select(undef, undef, undef, 0.01); # sleep a little

    }

    $$psql{stdout} = '';

    return 1;
}

sub try_to_set_hint_bits {
    my ($node) = @_;
    # Try to set hint bits in index on standby
    foreach (0 .. 10) {
        $node->safe_psql('postgres',
            'SELECT * FROM test_table WHERE value = 0 ORDER BY id LIMIT 1;');
    }
}

sub wait_for_catchup_all {
    $node_primary->wait_for_catchup($node_standby_1, 'replay',
        $node_primary->lsn('insert'));
    $node_standby_1->wait_for_catchup($node_standby_2, 'replay',
        $node_standby_1->lsn('replay'));
}

sub hints_num {
    my ($node) = @_;
    return $node->safe_psql('postgres',
        "SELECT count(*) FROM bt_page_items('test_index', 1) WHERE dead = true");
}

sub btp_safe_on_stanby {
    # BTP_LP_SAFE_ON_STANDBY (1 << 9)
    my ($node) = @_;
    if ($node->safe_psql('postgres',
        "SELECT btpo_flags FROM bt_page_stats('test_index', 1);") & (1 << 9)) {
        return 1
    } else {
        return 0
    }
}

sub non_normal_num {
    my ($node) = @_;
    return $node->safe_psql('postgres',
        "SELECT COUNT(*) FROM heap_page_items(get_raw_page('test_table', 0)) WHERE lp_flags != 1");
}