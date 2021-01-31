# Tests for logical replication table syncing
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 10;
use Time::HiRes qw(usleep);
use Scalar::Util qw(looks_like_number);

# Initialize publisher node
my $node_publisher = get_new_node('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

# Create subscriber node
my $node_subscriber = get_new_node('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->append_conf('postgresql.conf',
	"wal_retrieve_retry_interval = 1ms");
$node_subscriber->start;

# Create some preexisting content on publisher
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab_rep (a int primary key)");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rep SELECT generate_series(1,10)");

# Setup structure on subscriber
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab_rep (a int primary key)");

# Setup logical replication
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub FOR ALL TABLES");

$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub"
);

$node_publisher->wait_for_catchup('tap_sub');

# Also wait for initial table sync to finish
my $synced_query =
  "SELECT count(1) = 0 FROM pg_subscription_rel WHERE srsubstate NOT IN ('r', 's');";
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

my $result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM tab_rep");
is($result, qq(10), 'initial data synced for first sub');

# drop subscription so that there is unreplicated data
$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub");

$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rep SELECT generate_series(11,20)");

# recreate the subscription, it will try to do initial copy
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub"
);

# but it will be stuck on data copy as it will fail on constraint
my $started_query = "SELECT srsubstate = 'd' FROM pg_subscription_rel;";
$node_subscriber->poll_query_until('postgres', $started_query)
  or die "Timed out while waiting for subscriber to start sync";

# remove the conflicting data
$node_subscriber->safe_psql('postgres', "DELETE FROM tab_rep;");

# wait for sync to finish this time
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

# check that all data is synced
$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM tab_rep");
is($result, qq(20), 'initial data synced for second sub');

# now check another subscription for the same node pair
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub2 CONNECTION '$publisher_connstr' PUBLICATION tap_pub WITH (copy_data = false)"
);

# wait for it to start
$node_subscriber->poll_query_until('postgres',
	"SELECT pid IS NOT NULL FROM pg_stat_subscription WHERE subname = 'tap_sub2' AND relid IS NULL"
) or die "Timed out while waiting for subscriber to start";

# and drop both subscriptions
$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub");
$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub2");

# check subscriptions are removed
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM pg_subscription");
is($result, qq(0), 'second and third sub are dropped');

# remove the conflicting data
$node_subscriber->safe_psql('postgres', "DELETE FROM tab_rep;");

# recreate the subscription again
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub"
);

# and wait for data sync to finish again
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

# check that all data is synced
$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM tab_rep");
is($result, qq(20), 'initial data synced for fourth sub');

# add new table on subscriber
$node_subscriber->safe_psql('postgres', "CREATE TABLE tab_rep_next (a int)");

# setup structure with existing data on publisher
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab_rep_next (a) AS SELECT generate_series(1,10)");

$node_publisher->wait_for_catchup('tap_sub');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM tab_rep_next");
is($result, qq(0), 'no data for table added after subscription initialized');

# ask for data sync
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub REFRESH PUBLICATION");

# wait for sync to finish
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM tab_rep_next");
is($result, qq(10),
	'data for table added after subscription initialized are now synced');

# Add some data
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_rep_next SELECT generate_series(1,10)");

$node_publisher->wait_for_catchup('tap_sub');

$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM tab_rep_next");
is($result, qq(20),
	'changes for table added after subscription initialized replicated');

##
## slot integrity
##
## Manually create a slot with the same name that tablesync will want.
## Expect tablesync ERROR when clash is detected.
## Then remove the slot so tablesync can proceed.
## Expect tablesync can now finish normally.
##

# drop the subscription
$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub");

# empty the table tab_rep
$node_subscriber->safe_psql('postgres', "DELETE FROM tab_rep;");

# empty the table tab_rep_next
$node_subscriber->safe_psql('postgres', "DELETE FROM tab_rep_next;");

# recreate the subscription again, but leave it disabled so that we can get the OID
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub
	with (enabled = false)"
);

# need to create the name of the tablesync slot, for this we need the subscription OID
# and the table OID.
my $subid = $node_subscriber->safe_psql('postgres',
	"SELECT oid FROM pg_subscription WHERE subname = 'tap_sub';");
is(looks_like_number($subid), qq(1), 'get the subscription OID');

my $relid = $node_subscriber->safe_psql('postgres',
	"SELECT 'tab_rep_next'::regclass::oid");
is(looks_like_number($relid), qq(1), 'get the table OID');

# name of the tablesync slot is pg_'suboid'_sync_'tableoid'.
my $slotname = 'pg_' . $subid . '_' . 'sync_' . $relid;

# temporarily, create a slot having the same name of the tablesync slot.
$node_publisher->safe_psql('postgres',
	"SELECT 'init' FROM pg_create_logical_replication_slot('$slotname', 'pgoutput', false);");

# enable the subscription
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub ENABLE"
);

# check for occurrence of the expected error
poll_output_until("replication slot \"$slotname\" already exists")
    or die "no error stop for the pre-existing origin";

# now drop the offending slot, the tablesync should recover.
$node_publisher->safe_psql('postgres',
	"SELECT pg_drop_replication_slot('$slotname');");

# wait for sync to finish
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM tab_rep_next");
is($result, qq(20),
	'data for table added after subscription initialized are now synced');

# Cleanup
$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub");

$node_subscriber->stop('fast');
$node_publisher->stop('fast');

sub poll_output_until
{
    my ($expected) = @_;

    $expected = 'xxxxxx' unless defined($expected); # default junk value

    my $max_attempts = 10 * 10;
    my $attempts     = 0;

    my $output_file = '';
    while ($attempts < $max_attempts)
    {
        $output_file = slurp_file($node_subscriber->logfile());

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
