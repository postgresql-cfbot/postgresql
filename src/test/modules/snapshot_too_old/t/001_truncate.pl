# Test truncation of the old snapshot time mapping, to check
# that we can't get into trouble when xids wrap around.
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 6;

my $node = get_new_node('master');
$node->init;
$node->append_conf("postgresql.conf", "timezone = UTC");
$node->append_conf("postgresql.conf", "old_snapshot_threshold=10");
$node->append_conf("postgresql.conf", "max_prepared_transactions=10");
$node->append_conf("postgresql.conf", "autovacuum=off");
$node->start;
$node->psql('postgres', 'update pg_database set datallowconn = true');
$node->psql('postgres', 'create extension old_snapshot');
$node->psql('postgres', 'create extension test_sto');

note "check time map is truncated when CLOG is";

sub set_time
{
	my $time = shift;
	$node->psql('postgres', "select test_sto_clobber_snapshot_timestamp('$time')");
}

sub advance_xid
{
	my $time = shift;
	$node->psql('postgres', "select pg_current_xact_id()");
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

sub vacuum_freeze_all
{
	$node->psql('postgres', 'vacuum freeze');
	$node->psql('template0', 'vacuum freeze');
	$node->psql('template1', 'vacuum freeze');
}

# build up a time map with 4 entries
set_time('3000-01-01 00:00:00Z');
advance_xid();
set_time('3000-01-01 00:01:00Z');
advance_xid();
set_time('3000-01-01 00:02:00Z');
advance_xid();
set_time('3000-01-01 00:03:00Z');
advance_xid();
is(summarize_mapping(), "4|00:00:00|00:03:00");

# now cause frozen XID to advance
vacuum_freeze_all();

# we expect all XIDs to have been truncated
is(summarize_mapping(), "0||");

# put two more in the map
set_time('3000-01-01 00:04:00Z');
advance_xid();
set_time('3000-01-01 00:05:00Z');
advance_xid();
is(summarize_mapping(), "2|00:04:00|00:05:00");

# prepare a transaction, to stop xmin from getting further ahead
$node->psql('postgres', "begin; select pg_current_xact_id(); prepare transaction 'tx1'");

# add 16 more minutes; we should now have 18
set_time('3000-01-01 00:21:00Z');
advance_xid();
is(summarize_mapping(), "18|00:04:00|00:21:00");

# now cause frozen XID to advance
vacuum_freeze_all();

# this should leave just 16, because 2 were truncated
is(summarize_mapping(), "16|00:06:00|00:21:00");

# commit tx1, and then freeze again to get rid of all of them
$node->psql('postgres', "commit prepared 'tx1'");

# now cause frozen XID to advance
vacuum_freeze_all();

# we should now be back to empty
is(summarize_mapping(), "0||");

$node->stop;
