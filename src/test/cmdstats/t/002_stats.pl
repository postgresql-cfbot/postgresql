# Test of cmdstats system view
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 10;

my ($node, $result);

$node = get_new_node("cmdstats_test");
$node->init;
$node->append_conf('postgresql.conf', "cmdstats_tracking = true");
$node->start;

# Reset the stats.  Beware that this in itself is a SELECT statement, and it gets counted *after* the reset.
$result = $node->safe_psql('postgres', "SELECT pg_stat_reset_shared('cmdstats')");

# Check that we see the one select from above.  Note that this, too, is a SELECT that gets counted *after* it runs
$result = $node->safe_psql('postgres', "SELECT tag || ':' || cnt::text FROM pg_command_stats WHERE tag = 'SELECT'");
is($result, 'SELECT:1', 'have run one SELECT statement');

# Check that we see both selects from above
$result = $node->safe_psql('postgres', "SELECT tag || ':' || cnt::text FROM pg_command_stats WHERE tag = 'SELECT'");
is($result, 'SELECT:2', 'have run two SELECT statements');

# Check that we don't see any other kinds of statements
$result = $node->safe_psql('postgres', "SELECT COUNT(DISTINCT tag) FROM pg_command_stats");
is($result, '1', 'have run only kind of statement');

# Run a CREATE TABLE statement
$result = $node->safe_psql('postgres', "CREATE TABLE tst (i INTEGER)");

# Check that we now see both SELECT and CREATE TABLE
$result = $node->safe_psql('postgres', "SELECT cnt FROM pg_command_stats WHERE tag = 'SELECT'");
is($result, '4', 'have run four SELECT statements');
$result = $node->safe_psql('postgres', "SELECT cnt FROM pg_command_stats WHERE tag = 'CREATE TABLE'");
is($result, '1', 'have run one CREATE TABLE statement');
$result = $node->safe_psql('postgres', "SELECT COUNT(DISTINCT tag) FROM pg_command_stats");
is($result, '2', 'have run two kinds of statements');

# Reset all counters.  Note that the SELECT which resets the counters will itself get counted
$node->safe_psql('postgres', "SELECT * FROM pg_stat_reset_shared('cmdstats')");
$result = $node->safe_psql('postgres', "SELECT cnt FROM pg_command_stats WHERE tag = 'SELECT'");
is($result, '1', 'have run one SELECT statement since resetting the counters');

# Check that multi-statement commands are all counted, not just one of them
$node->safe_psql('postgres', "BEGIN; SELECT 1; ROLLBACK; BEGIN; SELECT 2; ROLLBACK; BEGIN; SELECT 3; ROLLBACK");
$result = $node->safe_psql('postgres', "SELECT cnt FROM pg_command_stats WHERE tag = 'SELECT'");
is($result, '5', 'have run five SELECT statements since resetting the counters');
$result = $node->safe_psql('postgres', "SELECT cnt FROM pg_command_stats WHERE tag = 'BEGIN'");
is($result, '3', 'have run three BEGIN statements');
$result = $node->safe_psql('postgres', "SELECT cnt FROM pg_command_stats WHERE tag = 'ROLLBACK'");
is($result, '3', 'have run three ROLLBACK statements');
