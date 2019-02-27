use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 8;
use IPC::Run qw( start timeout ) ;


my $verbose = $ENV{PG_TAP_VERBOSE};

# Launch pg_recvlogical as a background proc and return the IPC::Run handle for it
# as well as the proc's
sub start_pg_recvlogical
{
    my ($node, $slotname, %params) = @_;
    my $stdout = my $stderr = '';
    my $timeout           = undef;
    my $timeout_exception = 'pg_recvlogical timed out';

    $timeout =
        IPC::Run::timeout($params{timeout}, exception => $timeout_exception)
        if (defined($params{timeout}));

    my @cmd = ("pg_recvlogical", "--verbose", "-S", "$slotname", "--no-loop", "--dbname", $node->connstr, "--start", "-f", "-");

    push @cmd, @{ $params{option} }
        if defined $params{option};

    diag "Running '@cmd'" if $verbose;

    my $proc = start \@cmd, '<', \undef, '2>', \$stderr, '>', \$stdout, $timeout;

    die $! unless defined($proc);

    sleep 5;

    if ($stdout ne "")
    {
        diag "#### Begin standard out\n" if $verbose;
        diag $stdout if $verbose;
        diag "\n#### End standard out\n" if $verbose;
    }

    if ($stderr ne "")
    {
        diag "#### Begin standard error\n" if $verbose;
        diag $stderr if $verbose;
        diag "\n#### End standard error\n" if $verbose;
    }

    if (wantarray)
    {
        return ($proc, \$stdout, \$stderr, $timeout);
    }
    else
    {
        return $proc;
    }
}

sub wait_for_start_streaming
{
    my ($node, $slotname) = @_;

    diag "waiting for " . $node->name . " start streaming by slot ".$slotname if $verbose;
    $node->poll_query_until('postgres', "select active from pg_replication_slots where slot_name = '$slotname';");
}

sub wait_for_stop_streaming
{
    my ($node, $slotname) = @_;

    diag "waiting for " . $node->name . " streaming by slot ".$slotname." will be stopped" if $verbose;
    $node->poll_query_until('postgres', "select not(active) from pg_replication_slots where slot_name = '$slotname';");
}

sub create_logical_replication_slot
{
    my ($node, $slotname, $outplugin) = @_;

    $node->safe_psql(
        "postgres",
        "select pg_drop_replication_slot('$slotname') where exists (select 1 from pg_replication_slots where slot_name = '$slotname');");

    $node->safe_psql('postgres',
        "SELECT pg_create_logical_replication_slot('$slotname', '$outplugin');"
    );
}

my ($proc);

# Initialize master node
my $node_master = get_new_node('master');
$node_master->init(allows_streaming => 1, has_archiving => 1);
$node_master->append_conf('postgresql.conf', "wal_level = 'logical'\n");
$node_master->append_conf('postgresql.conf', "max_replication_slots = 12\n");
$node_master->append_conf('postgresql.conf', "max_wal_senders = 12\n");
$node_master->append_conf('postgresql.conf', "max_connections = 20\n");
$node_master->dump_info;
$node_master->start;


#TestCase 1: client initialize stop logical replication when database doesn't have new changes(calm state)
{
    my $slotname = 'calm_state_slot';

    create_logical_replication_slot($node_master, $slotname, "test_decoding");

    my ($stdout, $stderr, $timeout);
    ($proc, $stdout, $stderr, $timeout) = start_pg_recvlogical(
        $node_master,
        $slotname,
        timeout => 60,
        extra_params => ['-o include-xids=false', '-o skip-empty-xacts=true']
    );

    wait_for_start_streaming($node_master, $slotname);

    my $cancelTime = time();
    $proc->signal("SIGINT");

    $proc->pump while $proc->pumpable;

    wait_for_stop_streaming($node_master, $slotname);

    my $spendTime = time() - $cancelTime;

    my $timed_out = 0;
    eval {
        $proc->finish;
    };
    if ($@)
    {
        my $x = $@;
        if ($timeout->is_expired)
        {
            diag "whoops, pg_recvlogical timed out" if $verbose;
            $timed_out = 1;
        }
        else
        {
            die $x;
        }
    }

    if ($verbose)
    {
        diag "#--- pg_recvlogical stderr ---";
        diag $$stderr;
        diag "#--- end stderr ---";
    }

    ok(!$timed_out, "pg_recvlogical exited before timeout when idle");
    like($$stderr, qr/stopping write up to/, 'pg_recvlogical responded to sigint when idle');
    like($$stderr, qr/streaming ended by user request/, 'idle wait ended due to client copydone');
    diag "decoding when idle stopped after ${spendTime}s";
    ok((time() - $cancelTime) <= 3, # allow extra time for slow machines
        "pg_recvlogical exited promptly on sigint when idle"
    );
}


#TestCase 2: client initialize stop logical replication during decode huge transaction(insert 200000 records)
{
    my $slotname = 'huge_tx_state_slot';

    create_logical_replication_slot($node_master, $slotname, "test_decoding");

    $node_master->safe_psql('postgres',
        "create table test_logic_table(pk serial primary key, name varchar(100));");

    diag 'Insert huge amount of data to table test_logic_table' if $verbose;
    $node_master->safe_psql('postgres',
        "insert into test_logic_table select id, md5(random()::text) as name from generate_series(1, 200000) as id;");

    my ($stdout, $stderr, $timeout);
    ($proc, $stdout, $stderr, $timeout) = start_pg_recvlogical(
        $node_master,
        $slotname,
        timeout => 60,
        extra_params => ['-o include-xids=false', '-o skip-empty-xacts=true']
    );

    wait_for_start_streaming($node_master, $slotname);

    my $cancelTime = time();
    $proc->signal("SIGINT");

    $proc->pump while $proc->pumpable;

    wait_for_stop_streaming($node_master, $slotname);

    my $spendTime = time() - $cancelTime;

    my $timed_out = 0;
    eval {
        $proc->finish;
    };
    if ($@)
    {
        my $x = $@;
        if ($timeout->is_expired)
        {
            diag "whoops, pg_recvlogical timed out" if $verbose;
            $timed_out = 1;
        }
        else
        {
            die $x;
        }
    }

    if ($verbose)
    {
        diag "#--- pg_recvlogical stderr ---";
        diag $$stderr;
        diag "#--- end stderr ---";
    }

    ok(!$timed_out, "pg_recvlogical exited before timeout when streaming");
    like($$stderr, qr/stopping write up to/, 'pg_recvlogical responded to sigint when streaming');
    like($$stderr, qr/streaming ended by user request/, 'streaming ended due to client copydone');
    diag "decoding of big xact stopped after ${spendTime}s";
    ok($spendTime <= 5, # allow extra time for slow machines
        "pg_recvlogical exited promptly on signal when decoding");
}
