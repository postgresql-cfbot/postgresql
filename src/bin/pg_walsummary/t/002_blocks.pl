# Copyright (c) 2021-2023, PostgreSQL Global Development Group

use strict;
use warnings;
use File::Compare;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Set up a new database instance.
my $node1 = PostgreSQL::Test::Cluster->new('node1');
$node1->init(has_archiving => 1, allows_streaming => 1);
$node1->append_conf('postgresql.conf', 'summarize_wal = on');
$node1->start;

# See what's been summarized up until now.
my $progress = $node1->safe_psql('postgres', <<EOM);
SELECT summarized_tli, summarized_lsn FROM pg_get_wal_summarizer_state()
EOM
my ($summarized_tli, $summarized_lsn) = split(/\|/, $progress);
note("before insert, summarized TLI $summarized_tli through $summarized_lsn");

# Create a table and insert a few test rows into it. VACUUM FREEZE it so that
# autovacuum doesn't induce any future modifications unexpectedly. Then
# trigger a checkpoint.
$node1->safe_psql('postgres', <<EOM);
CREATE TABLE mytable (a int, b text);
INSERT INTO mytable
SELECT
	g, random()::text||random()::text||random()::text||random()::text
FROM
	generate_series(1, 400) g;
VACUUM FREEZE;
CHECKPOINT;
EOM

# Wait for a new summary to show up.
$node1->poll_query_until('postgres', <<EOM);
SELECT EXISTS (
    SELECT * from pg_available_wal_summaries()
    WHERE tli = $summarized_tli AND end_lsn > '$summarized_lsn'
)
EOM

# Again check the progress of WAL summarization.
$progress = $node1->safe_psql('postgres', <<EOM);
SELECT summarized_tli, summarized_lsn FROM pg_get_wal_summarizer_state()
EOM
($summarized_tli, $summarized_lsn) = split(/\|/, $progress);
note("after insert, summarized TLI $summarized_tli through $summarized_lsn");

# Update a row in the first block of the table and trigger a checkpoint.
$node1->safe_psql('postgres', <<EOM);
UPDATE mytable SET b = 'abcdefghijklmnopqrstuvwxyz' WHERE a = 2;
CHECKPOINT;
EOM

# Again wait for a new summary to show up.
$node1->poll_query_until('postgres', <<EOM);
SELECT EXISTS (
    SELECT * from pg_available_wal_summaries()
    WHERE tli = $summarized_tli AND end_lsn > '$summarized_lsn'
)
EOM

# Figure out the exact details for the new sumamry file.
my $details = $node1->safe_psql('postgres', <<EOM);
SELECT tli, start_lsn, end_lsn from pg_available_wal_summaries()
	WHERE tli = $summarized_tli AND end_lsn > '$summarized_lsn'
EOM
my ($tli, $start_lsn, $end_lsn) = split(/\|/, $details);
note("examining summary for TLI $tli from $start_lsn to $end_lsn");

# Reconstruct the full pathname for the WAL summary file.
my $filename = sprintf "%s/pg_wal/summaries/%08s%08s%08s%08s%08s.summary",
					   $node1->data_dir, $tli,
					   split(m@/@, $start_lsn),
					   split(m@/@, $end_lsn);
ok(-f $filename, "WAL summary file exists");

# Run pg_walsummary on it. We expect block 0 to be modified, but block 1
# to be unmodified, so the output should say block 0, not block 0..1 or
# similar.
my ($stdout, $stderr) = run_command([ 'pg_walsummary', $filename ]);
like($stdout, qr/FORK main: block 0$/m, "stdout shows block 0 modified");
is($stderr, '', 'stderr is empty');

done_testing();
