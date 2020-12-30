# Test xid various time/xid map maintenance edge cases
# that were historically buggy.

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 8;

my $node = get_new_node('main');
$node->init;
$node->append_conf("postgresql.conf", "timezone = UTC");
$node->append_conf("postgresql.conf", "old_snapshot_threshold=10");
$node->append_conf("postgresql.conf", "autovacuum = off");
$node->start;
$node->psql('postgres', 'create extension test_sto');
$node->psql('postgres', 'create extension old_snapshot');

sub set_time
{
	my $time = shift;
	$node->psql('postgres', "select test_sto_clobber_snapshot_timestamp('$time')");
}

sub summarize_mapping
{
	my $out;
	$node->psql('postgres',
				"select count(*),
						to_char(min(end_timestamp), 'HH24:MI:SS'),
						to_char(max(end_timestamp), 'HH24:MI:SS')
						from pg_old_snapshot_time_mapping()",
				stdout => \$out);
	return $out;
}

# fill the map up to maximum capacity
set_time('3000-01-01 00:00:00Z');
set_time('3000-01-01 00:19:00Z');
is(summarize_mapping(), "20|00:00:00|00:19:00");

# make a jump larger than capacity; the mapping is blown away,
# and our new minute is now the only one
set_time('3000-01-01 02:00:00Z');
is(summarize_mapping(), "1|02:00:00|02:00:00");

# test adding minutes while the map is not full
set_time('3000-01-01 02:01:00Z');
is(summarize_mapping(), "2|02:00:00|02:01:00");
set_time('3000-01-01 02:05:00Z');
is(summarize_mapping(), "6|02:00:00|02:05:00");
set_time('3000-01-01 02:19:00Z');
is(summarize_mapping(), "20|02:00:00|02:19:00");

# test adding minutes while the map is full
set_time('3000-01-01 02:20:00Z');
is(summarize_mapping(), "20|02:01:00|02:20:00");
set_time('3000-01-01 02:22:00Z');
is(summarize_mapping(), "20|02:03:00|02:22:00");
set_time('3000-01-01 02:22:01Z'); # one second past
is(summarize_mapping(), "20|02:04:00|02:23:00");

$node->stop;
