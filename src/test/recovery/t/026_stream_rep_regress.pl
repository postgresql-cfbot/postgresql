# Run the standard regression tests with streaming replication
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 2;

# Initialize primary node
my $node_primary = get_new_node('primary');
$node_primary->init(
	allows_streaming => 1);

# WAL consistency checking is resource intensive so require opt-in with the
# PG_TEST_EXTRA environment variable.
if ($ENV{PG_TEST_EXTRA} &&
	$ENV{PG_TEST_EXTRA} =~ m/\bwal_consistency_checking\b/) {
	$node_primary->append_conf('postgresql.conf',
		'wal_consistency_checking = all');
}

$node_primary->start;
is( $node_primary->psql(
        'postgres',
        qq[SELECT pg_create_physical_replication_slot('standby_1');]),
    0,
    'physical slot created on primary');
my $backup_name = 'my_backup';

# Take backup
$node_primary->backup($backup_name);

# Create streaming standby linking to primary
my $node_standby_1 = get_new_node('standby_1');
$node_standby_1->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_standby_1->append_conf('postgresql.conf',
    "primary_slot_name = standby_1");
$node_standby_1->start;

# XXX The tablespace tests don't currently work when the standby shares a
# filesystem with the primary, due to colliding absolute paths.  We'll skip
# that for now.

# Run the regression tests against the primary.
system_or_bail("../regress/pg_regress",
			   "--port=" . $node_primary->port,
			   "--schedule=../regress/parallel_schedule",
			   "--dlpath=../regress",
			   "--inputdir=../regress",
			   "--skip-tests=tablespace");

# Wait for standby to catch up
$node_primary->wait_for_catchup($node_standby_1, 'replay',
	$node_primary->lsn('insert'));

ok(1, "caught up");

$node_standby_1->stop;
$node_primary->stop;
