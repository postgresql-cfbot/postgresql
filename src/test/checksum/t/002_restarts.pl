# Test suite for testing enabling data checksums in an online cluster with
# restarting the processing
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;
use IPC::Run qw(pump finish timer);

# If we don't have IO::Pty, forget it, because IPC::Run depends on that
# to support pty connections
eval { require IO::Pty; };
if ($@)
{
	plan skip_all => 'IO::Pty is needed to run this test';
}

# Initialize node with checksums disabled.
my $node = get_new_node('main');
$node->init();
$node->start();

# Create some content to have un-checksummed data in the cluster
$node->safe_psql('postgres',
	"CREATE TABLE t AS SELECT generate_series(1,10000) AS a;");

# Ensure that checksums are disabled
my $result = $node->safe_psql('postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';"
);
is($result, 'off', 'ensure checksums are disabled');

# Create a barrier for checksumming to block on, in this case a pre-existing
# temporary table which is kept open while processing is started. We can
# accomplish this by setting up an interactive psql process which keeps the
# temporary table created as we enable checksums in another psql process.
my $in    = '';
my $out   = '';
my $timer = timer(5);

my $h = $node->interactive_psql('postgres', \$in, \$out, $timer);

$out = '';
$timer->start(5);

$in .= "CREATE TEMPORARY TABLE tt (a integer);\n";
pump $h until ($out =~ /CREATE TABLE/ || $timer->is_expired);

# In another session, make sure we can see the blocking temp table but start
# processing anyways and check that we are blocked with a proper wait event.
$result = $node->safe_psql('postgres',
	"SELECT relpersistence FROM pg_catalog.pg_class WHERE relname = 'tt';");
is($result, 't', 'ensure we can see the temporary table');

$node->safe_psql('postgres', "SELECT pg_enable_data_checksums();");

$result = $node->poll_query_until(
	'postgres',
	"SELECT count(*) FROM pg_catalog.pg_class WHERE NOT relhaschecksums "
	  . "AND relkind IN ('r', 'i', 'S', 't', 'm');",
	'1');
is($result, 1, 'ensure there is a single table left');

$result = $node->safe_psql('postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';"
);
is($result, 'inprogress-on', "ensure checksums aren't enabled yet");

$result = $node->safe_psql('postgres',
	"SELECT wait_event FROM pg_stat_activity WHERE backend_type = 'datachecksumsworker worker';"
);
is($result, 'ChecksumEnableFinishCondition', 'test for correct wait event');

$result = $node->safe_psql('postgres',
		"SELECT count(*) FROM pg_catalog.pg_class WHERE NOT relhaschecksums "
	  . "AND relkind IN ('r', 'i', 'S', 't', 'm');");
is($result, '1',
	'doublecheck that there is a single table left before restarting');

$node->stop;
$node->start;

$result = $node->safe_psql('postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';"
);
is($result, 'inprogress-on', "ensure checksums aren't enabled yet");

$result = $node->safe_psql('postgres',
		"SELECT count(*) FROM pg_catalog.pg_class WHERE NOT relhaschecksums "
	  . "AND relkind IN ('r', 'i', 'S', 't', 'm');");
is($result, '0', 'no temporary tables this time around');

$node->safe_psql('postgres', "SELECT pg_enable_data_checksums();");

$result = $node->poll_query_until(
	'postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';",
	'on');
is($result, 1, 'ensure checksums are turned on');

$result = $node->safe_psql('postgres', "SELECT count(*) FROM t");
is($result, '10000', 'ensure checksummed pages can be read back');

$result = $node->poll_query_until(
	'postgres',
	"SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE 'datachecksumsworker%';",
	'0');
is($result, 1, 'await datachecksums worker/launcher termination');

$result = $node->safe_psql('postgres', "SELECT pg_disable_data_checksums();");
$result = $node->poll_query_until(
	'postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';",
	'off');
is($result, 1, 'ensure checksums are turned off');

done_testing();
