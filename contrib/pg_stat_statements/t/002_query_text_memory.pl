# Copyright (c) 2008-2026, PostgreSQL Global Development Group

# Tests for pg_stat_statements.query_text_memory behavior.
# Verifies that when the query text DSA is exhausted:
#   - entries are still tracked with counters accumulating
#   - query text is NULL for entries that could not store text
#   - both showtext=true and showtext=false return all entries
#   - after raising the limit and re-executing, text is backfilled

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_statements'
pg_stat_statements.query_text_memory = 2MB
});
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION pg_stat_statements');
$node->safe_psql('postgres', 'SELECT pg_stat_statements_reset()');

my $mem = $node->safe_psql('postgres',
	"SHOW pg_stat_statements.query_text_memory");
is($mem, '2MB', 'query_text_memory is 2MB');

# Generate unique queries to exhaust the text DSA (set to 2MB).
# Each CTE query with 200 integer constants is ~1000 bytes of text.
my $cols = join(', ', map { "$_" } (1 .. 200));
my $sql = '';
for my $i (1 .. 2500)
{
	$sql .= "WITH t${i} AS (SELECT $cols) SELECT FROM t${i};\n";
}
$node->safe_psql('postgres', $sql);

my $null_count = $node->safe_psql('postgres', q{
SELECT count(*)
FROM pg_stat_statements
WHERE query IS NULL AND queryid IS NOT NULL
});

diag("$null_count entries without text after 2500 queries");

cmp_ok($null_count, '>', 0,
	"some entries have NULL query text after DSA exhaustion ($null_count)");

# Entries without text still have calls > 0
my $tracked = $node->safe_psql('postgres', q{
SELECT count(*)
FROM pg_stat_statements
WHERE query IS NULL AND queryid IS NOT NULL AND calls > 0
});
cmp_ok($tracked, '>', 0,
	"entries without text still track counters ($tracked)");

# Entries with text also exist (early entries got text before exhaustion)
my $with_text = $node->safe_psql('postgres', q{
SELECT count(*)
FROM pg_stat_statements
WHERE query IS NOT NULL AND query LIKE 'WITH t%'
});
cmp_ok($with_text, '>', 0,
	"some entries still have query text ($with_text)");

# Both showtext=true and showtext=false should return all entries
my $count_true = $node->safe_psql('postgres', q{
SELECT count(*) FROM pg_stat_statements(true)
});
my $count_false = $node->safe_psql('postgres', q{
SELECT count(*) FROM pg_stat_statements(false)
});
cmp_ok($count_true, '>=', 2500,
	"showtext=true returns all entries ($count_true)");
cmp_ok($count_false, '>=', 2500,
	"showtext=false returns all entries ($count_false)");

# Run a probe query with constants while DSA is exhausted.
# Since nentries < pg_stat_statements.max, entry_dealloc won't evict or
# free any DSA space, so this entry should remain with NULL text.
$node->safe_psql('postgres', 'SELECT 11111 + 22222 + 33333');

my $probe_before = $node->safe_psql('postgres', q{
SELECT count(*) FROM pg_stat_statements WHERE query = 'SELECT $1 + $2 + $3'
});
is($probe_before, '0',
	'probe query text is NULL while DSA is exhausted');

# Phase 2: Raise limit and verify backfill.
$node->safe_psql('postgres',
	"ALTER SYSTEM SET pg_stat_statements.query_text_memory = '100MB'");
$node->safe_psql('postgres', "SELECT pg_reload_conf()");

# Re-run the probe query in a new connection to trigger backfill.
# Normalization is not guaranteed for all backfills (e.g. when triggered
# from ExecutorEnd where jstate is unavailable), but when the backfill
# occurs via post_parse_analyze, jstate is available and the text should
# be stored in normalized form.
$node->safe_psql('postgres', 'SELECT 11111 + 22222 + 33333');

my $norm_after = $node->safe_psql('postgres', q{
SELECT query FROM pg_stat_statements WHERE query = 'SELECT $1 + $2 + $3'
});
is($norm_after, 'SELECT $1 + $2 + $3',
	'backfilled text is stored in normalized form');

# Also re-run the bulk queries to trigger their backfill
$node->safe_psql('postgres', $sql);

my $null_after = $node->safe_psql('postgres', q{
SELECT count(*)
FROM pg_stat_statements
WHERE query IS NULL AND queryid IS NOT NULL
});
diag("after backfill: $null_after entries without text");
cmp_ok($null_after, '<', $null_count,
	"backfill reduced NULL text entries ($null_count -> $null_after)");

$node->stop;
done_testing();
