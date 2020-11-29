#
# Test situation where a target data directory contains
# WAL records beyond both the last checkpoint and the divergence
# point.
#
# This test does not make use of RewindTest as it requires three
# nodes.

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 2;

my $node_1 = get_new_node('node_1');
$node_1->init(allows_streaming => 1);
$node_1->append_conf(
		'postgresql.conf', qq(
wal_keep_size=16
));

$node_1->start;

# Add an arbitrary table
$node_1->safe_psql('postgres',
	'CREATE TABLE public.foo (id INT)');

# Take backup
my $backup_name = 'my_backup';
$node_1->backup($backup_name);

# Create streaming standby from backup
my $node_2 = get_new_node('node_2');
$node_2->init_from_backup($node_1, $backup_name,
	has_streaming => 1);

$node_2->append_conf(
		'postgresql.conf', qq(
wal_keep_size=16
));

$node_2->start;

# Create streaming standby from backup
my $node_3 = get_new_node('node_3');
$node_3->init_from_backup($node_1, $backup_name,
	has_streaming => 1);

$node_3->append_conf(
		'postgresql.conf', qq(
wal_keep_size=16
));

$node_3->start;

# Stop node_1

$node_1->stop('fast');

# Promote node_3
$node_3->promote;

# node_1 rejoins node_3

my $node_3_connstr = $node_3->connstr;

$node_1->append_conf(
		'postgresql.conf', qq(
primary_conninfo='$node_3_connstr'
));
$node_1->set_standby_mode();
$node_1->start();

# node_2 follows node_3

$node_2->append_conf(
		'postgresql.conf', qq(
primary_conninfo='$node_3_connstr'
));
$node_2->restart();

# Promote node_1

$node_1->promote;

# We now have a split-brain with two primaries. Insert a row on both to
# demonstratively create a split brain; this is not strictly necessary
# for this test, but creates an easily identifiable WAL record and
# enables us to verify that node_2 has the required changes to
# reproduce the situation we're handling.

$node_1->safe_psql('postgres', 'INSERT INTO public.foo (id) VALUES (0)');
$node_3->safe_psql('postgres', 'INSERT INTO public.foo (id) VALUES (1)');

$node_2->poll_query_until('postgres',
	q|SELECT COUNT(*) > 0 FROM public.foo|, 't');

# At this point node_2 will shut down without a shutdown checkpoint,
# but with WAL entries beyond the preceding shutdown checkpoint.
$node_2->stop('fast');
$node_3->stop('fast');


my $node_2_pgdata = $node_2->data_dir;
my $node_1_connstr = $node_1->connstr;

command_checks_all(
    [
        'pg_rewind',
        "--source-server=$node_1_connstr",
        "--target-pgdata=$node_2_pgdata",
        '--dry-run'
    ],
    0,
    [],
    [qr|rewinding from last common checkpoint at|],
    'pg_rewind detects rewind needed');

