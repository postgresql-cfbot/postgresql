# Copyright (c) 2023-2026, PostgreSQL Global Development Group
#
# Test XID wraparound limits.
#
# When you get close to XID wraparound, you start to get warnings, and
# when you get even closer, the system refuses to assign any more XIDs
# until the oldest databases have been vacuumed and datfrozenxid has
# been advanced.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Session;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

if (!$ENV{PG_TEST_EXTRA} || $ENV{PG_TEST_EXTRA} !~ /\bxid_wraparound\b/)
{
	plan skip_all => "test xid_wraparound not enabled in PG_TEST_EXTRA";
}

my $ret;

# Initialize node
my $node = PostgreSQL::Test::Cluster->new('wraparound');

$node->init;
$node->append_conf(
	'postgresql.conf', qq[
autovacuum_naptime = 1s
log_autovacuum_min_duration = 0
log_connections = on
log_statement = 'all'
]);
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION xid_wraparound');

# Create a test table. We disable autovacuum on the table to run it only
# to prevent wraparound.
$node->safe_psql(
	'postgres', qq[
CREATE TABLE wraparoundtest(t text) WITH (autovacuum_enabled = off);
INSERT INTO wraparoundtest VALUES ('start');
]);

# Start a background session, which holds a transaction open, preventing
# autovacuum from advancing relfrozenxid and datfrozenxid.  Bump the query
# timeout to avoid false negatives on slow test systems.
my $background_session = PostgreSQL::Test::Session->new(node => $node,
	timeout => 4 * $PostgreSQL::Test::Utils::timeout_default);
$background_session->do(
	qq[
	BEGIN;
	INSERT INTO wraparoundtest VALUES ('oldxact');
]);

# Consume 2 billion transactions, to get close to wraparound
$node->safe_psql('postgres', qq[SELECT consume_xids(1000000000)]);
$node->safe_psql('postgres',
	qq[INSERT INTO wraparoundtest VALUES ('after 1 billion')]);

$node->safe_psql('postgres', qq[SELECT consume_xids(1000000000)]);
$node->safe_psql('postgres',
	qq[INSERT INTO wraparoundtest VALUES ('after 2 billion')]);

# We are now just under 150 million XIDs away from wraparound.
# Continue consuming XIDs, in batches of 10 million, until we get
# the warning:
#
#  WARNING:  database "postgres" must be vacuumed within 3000024 transactions
#  HINT:  To avoid a database shutdown, execute a database-wide VACUUM in that database.
#  You might also need to commit or roll back old prepared transactions, or drop stale replication slots.
my $stderr;
my $warn_limit = 0;
for my $i (1 .. 15)
{
	$node->psql(
		'postgres', qq[SELECT consume_xids(10000000)],
		stderr => \$stderr,
		on_error_die => 1);

	if ($stderr =~
		/WARNING:  database "postgres" must be vacuumed within [0-9]+ transactions/
	  )
	{
		# Reached the warn-limit
		$warn_limit = 1;
		last;
	}
}
is($warn_limit, 1, "warn-limit reached");

# We can still INSERT, despite the warnings.
$node->safe_psql('postgres',
	qq[INSERT INTO wraparoundtest VALUES ('reached warn-limit')]);

# Keep going. We'll hit the hard "stop" limit.
$ret = $node->psql(
	'postgres',
	qq[SELECT consume_xids(100000000)],
	stderr => \$stderr);
like(
	$stderr,
	qr/ERROR:  database is not accepting commands that assign new transaction IDs to avoid wraparound data loss in database "postgres"/,
	"stop-limit");

# Finish the old transaction, to allow vacuum freezing to advance
# relfrozenxid and datfrozenxid again.
$background_session->do(qq[COMMIT;]);
$background_session->close;

# VACUUM, to freeze the tables and advance datfrozenxid.
#
# Autovacuum does this for the other databases, and would do it for
# 'postgres' too, but let's test manual VACUUM.
#
$node->safe_psql('postgres', 'VACUUM');

# Wait until autovacuum has processed the other databases and advanced
# the system-wide oldest-XID.
$ret =
  $node->poll_query_until('postgres',
	qq[INSERT INTO wraparoundtest VALUES ('after VACUUM') RETURNING true],
						 );

# Check the table contents
$ret = $node->safe_psql('postgres', qq[SELECT * from wraparoundtest]);
is( $ret, "start
oldxact
after 1 billion
after 2 billion
reached warn-limit
after VACUUM");

$node->stop;
done_testing();
