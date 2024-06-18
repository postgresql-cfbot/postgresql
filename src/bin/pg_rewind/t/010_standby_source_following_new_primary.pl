
# Copyright (c) 2021-2023, PostgreSQL Global Development Group

#
# Test using a standby server that follows new primary as the source.
#
# This sets up three nodes: A, B and C. First, A is the primary,
# B follows A, and C follows B:
#
# A (primary) <--- B (standby) <--- C (standby)
#
#
# Then we promote B, and insert some divergent rows in A and B:
#
# A (primary)      B (primary) <--- C (standby)
#
#
# Finally, we run pg_rewind on A, to point it at C:
#
#                  B (primary) <--- C (standby) <--- A (standby)
#
# We was not able to rewind until checkpointing in this scenario due to
# the bug that cascade standby (i.e. C) does not follow the new
# primary's (i.e. B's) minRecoveryPoint and minRecoveryPointTLI.
#
# Since we're dealing with three nodes, we cannot use most of the
# RewindTest functions as is.

use strict;
use warnings;
use PostgreSQL::Test::Utils;
use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use File::Copy;
use PostgreSQL::Test::Cluster;
use RewindTest;

my $tmp_folder = PostgreSQL::Test::Utils::tempdir;

my $node_a;
my $node_b;
my $node_c;

# Set up node A, as primary
#
# A (primary)

setup_cluster('a');
start_primary();
$node_a = $node_primary;

# Create a test table and insert a row in primary.
$node_a->safe_psql('postgres', "CREATE TABLE tbl1 (d text)");
$node_a->safe_psql('postgres', "INSERT INTO tbl1 VALUES ('before promotion')");
primary_psql("CHECKPOINT");

# Set up node B and C, as cascaded standbys
#
# A (primary) <--- B (standby) <--- C (standby)
$node_a->backup('my_backup');
$node_b = PostgreSQL::Test::Cluster->new('node_b');
$node_b->init_from_backup($node_a, 'my_backup', has_streaming => 1);
$node_b->set_standby_mode();
$node_b->start;

$node_b->backup('my_backup');
$node_c = PostgreSQL::Test::Cluster->new('node_c');
$node_c->init_from_backup($node_b, 'my_backup', has_streaming => 1);
$node_c->set_standby_mode();
$node_c->start;

# Promote B
#
# A (primary)      B (primary) <--- C (standby)

$node_b->promote;

# make sure end-of-recovery record is replicated to C before we continue
$node_b->wait_for_catchup('node_c');

# Insert a row in A. This causes A and B/C to have "diverged", so that it's
# no longer possible to just apply the standy's logs over primary directory
# - you need to rewind.
$node_a->safe_psql('postgres',
	"INSERT INTO tbl1 VALUES ('rewind this')");

#
# All set up. We're ready to run pg_rewind.
#
my $node_a_pgdata = $node_a->data_dir;

# Stop the old primary node and be ready to perform the rewind.
$node_a->stop('fast');

# Keep a temporary postgresql.conf or it would be overwritten during the rewind.
copy(
	"$node_a_pgdata/postgresql.conf",
	"$tmp_folder/node_a-postgresql.conf.tmp");

{
	# Temporarily unset PGAPPNAME so that the server doesn't
	# inherit it.  Otherwise this could affect libpqwalreceiver
	# connections in confusing ways.
	local %ENV = %ENV;
	delete $ENV{PGAPPNAME};

	# Do rewind using a remote connection as source.
	command_ok(
		[
			'pg_rewind', "--debug",
			"--source-server", $node_c->connstr('postgres'),
			"--target-pgdata=$node_a_pgdata", "--no-sync",
		],
		'pg_rewind remote');
}

# Now move back postgresql.conf with old settings
move(
	"$tmp_folder/node_a-postgresql.conf.tmp",
	"$node_a_pgdata/postgresql.conf");

# Restart the node.
$node_a->start;

# set RewindTest::node_primary to point to the rewound node, so that we can
# use check_query()
$node_primary = $node_a;

# Verify that A has been successfully rewound.

check_query(
	'SELECT * FROM tbl1',
	qq(before promotion
),
	'table content after rewind');

# clean up
$node_a->teardown_node;
$node_b->teardown_node;
$node_c->teardown_node;

done_testing();

