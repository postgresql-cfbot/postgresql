# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that log_postmaster_stats causes the postmaster to periodically
# emit a "postmaster stats:" LOG line.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;
$node->append_conf('postgresql.conf', "log_postmaster_stats = 1");
$node->start;

my $offset = -s $node->logfile;

$node->safe_psql('postgres', 'SELECT 1');

# simply wait for line to appear in the log
$node->wait_for_log(qr/postmaster stats: avg .* conns\/sec/, $offset);
pass('postmaster stats line emitted at LOG level');
$node->stop;
done_testing();
