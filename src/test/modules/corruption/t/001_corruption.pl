use strict;
use warnings;

use TestLib;
use Test::More tests => 10;
use PostgresNode;

my $node = get_new_node('test');
$node->init;
$node->start;

# Create something non-trivial for the first snapshot
$node->safe_psql('postgres', qq(
create table t1 (id integer, short_text text, long_text text);
insert into t1 (id, short_text, long_text)
	(select gs, 'foo', repeat('x', gs)
		from generate_series(1,10000) gs);
create unique index idx1 on t1 (id, short_text);
vacuum freeze;
));

# Flush relation files to disk and take snapshot of them
$node->restart;
$node->take_relfile_snapshot('postgres', 'snap1', 'public.t1');

# Update data in the table, toast table, and index
$node->safe_psql('postgres', qq(
update t1 set
	short_text = 'bar',
	long_text = repeat('y', id);
));

# Flush relation files to disk and take second snapshot
$node->restart;
$node->take_relfile_snapshot('postgres', 'snap2', 'public.t1');

# Revert the first page of t1 using a torn snapshot.  This should be a partial
# and corrupt reverting of the update.
$node->stop;
$node->revert_to_torn_relfile_snapshot('snap1', 8192);

# Restart the node and count the number of rows in t1 with the original
# (pre-update) values.  It should not be zero, but nor will it be the full
# 10000.
$node->start;
my ($old, $new, $oldtoast, $newtoast) = counts();
ok($old > 0 && $old < 10000, "Torn snapshot reverts some of the main updates");
ok($new > 0 && $new <= 10000, "Torn snapshot retains some of the main updates");

# Revert t1 fully to the first snapshot.  This should fully restore the
# original (pre-update) values.
$node->stop;
$node->revert_to_snapshot('snap1');

# Restart the node and verify only old values remain
$node->start;
($old, $new, $oldtoast, $newtoast) = counts();
is($old, 10000, "Full snapshot restores all the old main values");
is($oldtoast, 10000, "Full snapshot restores all the old toast values");
is($new, 0, "Full snapshot reverts all the new main values");
is($newtoast, 0, "Full snapshot reverts all the new toast values");

# Restore t1 fully to the second snapshot.  This should fully restore the
# new (post-update) values.
$node->stop;
$node->revert_to_snapshot('snap2');

# Restart the node and verify only new values remain
$node->start;
($old, $new, $oldtoast, $newtoast) = counts();
is($old, 0, "Full snapshot reverts all the old main values");
is($oldtoast, 0, "Full snapshot reverts all the old toast values");
is($new, 10000, "Full snapshot restores all the new main values");
is($newtoast, 10000, "Full snapshot restores all the new toast values");

sub counts {
	return map {
		$node->safe_psql('postgres', qq(select count(*) from t1 where $_))
	} ("short_text = 'foo'",
	   "short_text = 'bar'",
	   "long_text ~ 'x'",
	   "long_text ~ 'y'");
}
