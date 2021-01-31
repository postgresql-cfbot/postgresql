# Test suite for testing enabling data checksums offline from various states
# of checksum processing
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

# Enable checksums offline using pg_checksums
$node->stop();
$node->checksum_enable_offline();
$node->start();

# Ensure that checksums are enabled
$result = $node->safe_psql('postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';"
);
is($result, 'on', 'ensure checksums are enabled');

# Run a dummy query just to make sure we can read back some data
$result = $node->safe_psql('postgres', "SELECT count(*) FROM t");
is($result, '10000', 'ensure checksummed pages can be read back');

# Disable checksums offline again using pg_checksums
$node->stop();
$node->checksum_disable_offline();
$node->start();

# Ensure that checksums are disabled
$result = $node->safe_psql('postgres',
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
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';",
	'inprogress-on');
is($result, 1, 'ensure checksums are in the process of being enabled');

# Turn the cluster off and enable checksums offline, then start back up
$node->stop();
$node->checksum_enable_offline();
$node->start();

# Ensure that checksums are now enabled even though processing wasn't
# restarted
$result = $node->safe_psql('postgres',
	"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';"
);
is($result, 'on', 'ensure checksums are enabled');

# Run a dummy query just to make sure we can read back some data
$result = $node->safe_psql('postgres', "SELECT count(*) FROM t");
is($result, '10000', 'ensure checksummed pages can be read back');

done_testing();
