# Copyright (c) 2025-2026, PostgreSQL Global Development Group
#
# Tests for the WAL pipeline feature (wal_pipeline GUC).

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# ----------
# Helpers
# ----------

sub slurp_log
{
	my ($node) = @_;
	open(my $fh, '<', $node->logfile()) or die "Cannot open log: $!";
	my @lines = <$fh>;
	close($fh);
	return @lines;
}

sub log_matches
{
	my ($node, $re) = @_;
	return grep { /$re/ } slurp_log($node);
}


# ########################################
#   wal_pipeline = on, basic recovery
# ########################################

my $node1 = PostgreSQL::Test::Cluster->new('p1-recovery');
$node1->init;
$node1->start;

$node1->safe_psql('postgres', q{
    CREATE TABLE t (id serial PRIMARY KEY, v text);
    INSERT INTO t (v)
    SELECT md5(i::text) FROM generate_series(1,50000) i;
});

# generate more WAL
$node1->safe_psql('postgres', q{
    INSERT INTO t (v)
    SELECT md5(i::text) FROM generate_series(1,50000) i;
});

# crash stop to force WAL recovery
$node1->stop('immediate');

# restart → recovery happens
$node1->append_conf('postgresql.conf', "wal_pipeline = on");
$node1->start;


# Producer started
ok(scalar log_matches($node1, qr/\[walpipeline\] producer: started at/),
	'producer started message found in log');

# Pipeline stopped cleanly
ok(scalar log_matches($node1, qr/\[walpipeline\] shutdown/),
	'pipeline stopped message found in log');

# Consumer received shutdown from producer
ok(scalar log_matches($node1, qr/\[walpipeline\] consumer: received shutdown message/),
	'consumer received shutdown message from producer');

# sent == received
my @exit_lines = log_matches($node1,
	qr/\[walpipeline\] producer: exiting: sent=\d+ received=\d+/);
ok(scalar @exit_lines >= 1, 'producer exiting line found in log');

my ($sent, $recv) = $exit_lines[-1] =~ /sent=(\d+) received=(\d+)/;
ok(defined $sent && $sent > 0, "sent count ($sent) is positive");
ok(defined $recv && $recv > 0, "received count ($recv) is positive");
is($sent, $recv, "no records lost in pipeline queue: sent=$sent received=$recv");

# No PANIC
ok(!(scalar log_matches($node1, qr/\bPANIC\b/)),
	'no PANIC messages during pipeline recovery');

# Data integrity
my $count = $node1->safe_psql('postgres', 'SELECT count(*) FROM t');
is($count + 0, 100_000, 'all 100000 rows visible after pipeline recovery');

$node1->stop;

# ##############################################################
#    wal_pipeline = off (baseline, no pipeline log messages)
# ##############################################################

my $node2 = PostgreSQL::Test::Cluster->new('p0-recovery');
$node2->init;
$node2->start;

$node2->safe_psql('postgres', q{
    CREATE TABLE t (id serial PRIMARY KEY, v text);
    INSERT INTO t (v)
    SELECT md5(i::text) FROM generate_series(1,50000) i;
});

# generate more WAL
$node2->safe_psql('postgres', q{
    INSERT INTO t (v)
    SELECT md5(i::text) FROM generate_series(1,50000) i;
});

# crash stop to force WAL recovery
$node2->stop('immediate');

# restart → recovery happens
$node2->append_conf('postgresql.conf', "wal_pipeline = off");
$node2->start;

ok(!(scalar log_matches($node2, qr/\[walpipeline\] producer: started/)),
	'no pipeline log messages when wal_pipeline = off');

my $count2 = $node2->safe_psql('postgres', 'SELECT count(*) FROM t');
is($count2 + 0, 100_000, 'all rows present after non-pipeline recovery');

$node2->stop;



# ###################################################################
#  Test pipeline on vs off produce identical data (checksum comparison)
# ###################################################################

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 1);
$primary->start;

$primary->safe_psql('postgres', q{
    CREATE TABLE t (id serial PRIMARY KEY, v text);
    INSERT INTO t (v)
    SELECT md5(i::text) FROM generate_series(1, 30000) i;
});

$primary->backup('backup3');

$primary->safe_psql('postgres', q{
    INSERT INTO t (v)
    SELECT md5(i::text) FROM generate_series(1, 30000) i;
    UPDATE t SET v = 'x' WHERE id % 10 = 0;
});

# ensure WAL boundary
$primary->safe_psql('postgres', 'SELECT pg_switch_wal()');
my $target_lsn = $primary->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

my $replica_on = PostgreSQL::Test::Cluster->new('replica_p1');
$replica_on->init_from_backup($primary, 'backup3',
    has_streaming => 1);
$replica_on->append_conf('postgresql.conf', "wal_pipeline = on\n");
$replica_on->start;

my $replica_off = PostgreSQL::Test::Cluster->new('replica_p0');
$replica_off->init_from_backup($primary, 'backup3',
    has_streaming => 1);
$replica_off->append_conf('postgresql.conf', "wal_pipeline = off\n");
$replica_off->start;

# wait for replicas to catch up
$primary->wait_for_catchup($replica_on);
$primary->wait_for_catchup($replica_off);

my $md5_on  = $replica_on->safe_psql('postgres',
    "SELECT md5(string_agg(id::text||v, ',' ORDER BY id)) FROM t");

my $md5_off = $replica_off->safe_psql('postgres',
    "SELECT md5(string_agg(id::text||v, ',' ORDER BY id)) FROM t");

is($md5_on, $md5_off,
    'table checksum identical between pipeline=on and pipeline=off');

$replica_on->stop;
$replica_off->stop;
$primary->stop('fast');



# #################################
#  Test pipeline when no need of replay
# #################################

my $node3 = PostgreSQL::Test::Cluster->new('p1-small-replay');
$node3->init;
$node3->start;

# crash stop to force WAL recovery
$node3->stop('immediate');

# restart → recovery happens
$node3->append_conf('postgresql.conf', "wal_pipeline = on");
$node3->start;

ok(scalar log_matches($node3, qr/\[walpipeline\] producer: exiting: sent=0 received=0/),
	'pipeline producer sent zero records');

ok((scalar log_matches($node3, qr/redo done at/)),
	'pipeline redo done even with tiny replay');

$node3->stop;

done_testing();