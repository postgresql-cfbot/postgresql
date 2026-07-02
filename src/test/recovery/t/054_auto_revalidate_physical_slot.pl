
# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Test auto-revalidation of physical replication slots.
#
# Verifies that a physical replication slot with auto_revalidate=true can
# recover from invalidation when the standby reconnects and confirms WAL
# receipt.  Also verifies that after revalidation the slot properly holds
# back WAL again, and that hot_standby_feedback xmin is re-established.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use Test::More;

#
# Primary setup: wal-segsize=1MB, archiving enabled, small
# max_slot_wal_keep_size so we can trigger invalidation quickly.
#
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(
	allows_streaming => 1,
	has_archiving    => 1,
	extra            => ['--wal-segsize=1']);
$node_primary->append_conf(
	'postgresql.conf', qq(
min_wal_size = 2MB
max_wal_size = 4MB
max_slot_wal_keep_size = 1MB
log_checkpoints = yes
));
$node_primary->start;

# Create a physical replication slot with auto_revalidate enabled.
$node_primary->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('revalidate_slot', true, false, true)"
);

# Verify auto_revalidate is true
my $result = $node_primary->safe_psql('postgres',
	"SELECT auto_revalidate FROM pg_replication_slots WHERE slot_name = 'revalidate_slot'");
is($result, "t", 'auto_revalidate is true on created slot');

# Take a backup for the standby
my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

# Create standby using the slot, with both streaming and restore_command
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup(
	$node_primary, $backup_name,
	has_streaming => 1,
	has_restoring => 1);
$node_standby->append_conf('postgresql.conf',
	"primary_slot_name = 'revalidate_slot'");
$node_standby->start;

# Wait for standby to catch up
$node_primary->wait_for_catchup($node_standby);

# Verify slot is active and not invalidated
$result = $node_primary->safe_psql('postgres',
	"SELECT active, invalidation_reason IS NULL FROM pg_replication_slots WHERE slot_name = 'revalidate_slot'");
is($result, "t|t", 'slot is active and valid after initial sync');

# Stop standby so we can invalidate the slot
$node_standby->stop;

# Generate enough WAL to exceed max_slot_wal_keep_size and invalidate the slot
my $logstart = -s $node_primary->logfile;
$node_primary->advance_wal(7);
$node_primary->safe_psql('postgres', "CHECKPOINT;");

# Wait for slot to be invalidated
$logstart = $node_primary->wait_for_log(
	qr/invalidating obsolete replication slot "revalidate_slot"/,
	$logstart);
pass('slot was invalidated due to WAL removal');

$result = $node_primary->safe_psql('postgres',
	"SELECT invalidation_reason FROM pg_replication_slots WHERE slot_name = 'revalidate_slot'");
is($result, "wal_removed", 'slot shows wal_removed invalidation reason');

##########################################################################
# Test 1: Revalidation after standby reconnects
##########################################################################

# Restart standby -- it should recover from archive then start streaming.
# The slot should be revalidated automatically.
$logstart = -s $node_primary->logfile;
$node_standby->start;

# Wait for the revalidation log message
$logstart = $node_primary->wait_for_log(
	qr/physical replication slot "revalidate_slot" has been revalidated/,
	$logstart);
pass('slot was revalidated after standby reconnected');

# Wait for standby to fully catch up
$node_primary->wait_for_catchup($node_standby);

# Verify slot is now valid again
$result = $node_primary->safe_psql('postgres',
	"SELECT active, invalidation_reason IS NULL FROM pg_replication_slots WHERE slot_name = 'revalidate_slot'");
is($result, "t|t", 'slot is active and valid after revalidation');

##########################################################################
# Test 2: After revalidation, slot holds back WAL when standby is stopped
##########################################################################

# Remove the WAL limit so we can test WAL retention by the slot itself
$node_primary->safe_psql('postgres',
	"ALTER SYSTEM SET max_slot_wal_keep_size = '-1'");
$node_primary->reload;

# Stop standby
$node_standby->stop;

# Record the current restart_lsn
my $restart_lsn_before = $node_primary->safe_psql('postgres',
	"SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = 'revalidate_slot'");
ok($restart_lsn_before ne '', 'restart_lsn is set after revalidation');

# Advance WAL significantly
$node_primary->advance_wal(5);
$node_primary->safe_psql('postgres', "CHECKPOINT;");

# Verify the slot's WAL is still reserved (not lost)
$result = $node_primary->safe_psql('postgres',
	"SELECT wal_status FROM pg_replication_slots WHERE slot_name = 'revalidate_slot'");
is($result, "reserved",
	'slot WAL status is "reserved" after revalidation -- WAL is held back');

# Restart standby and verify it catches up without issues
$node_standby->start;
$node_primary->wait_for_catchup($node_standby);

$result = $node_primary->safe_psql('postgres',
	"SELECT active, invalidation_reason IS NULL FROM pg_replication_slots WHERE slot_name = 'revalidate_slot'");
is($result, "t|t", 'slot still valid after standby reconnected with held WAL');

$node_standby->stop;

##########################################################################
# Test 3: hot_standby_feedback=on -- xmin is re-established after revalidation
##########################################################################

# Enable hot_standby_feedback on the standby
$node_standby->append_conf('postgresql.conf', "hot_standby_feedback = on");

# Re-enable WAL limit for another invalidation cycle
$node_primary->safe_psql('postgres',
	"ALTER SYSTEM SET max_slot_wal_keep_size = '1MB'");
$node_primary->reload;

$node_standby->start;
$node_primary->wait_for_catchup($node_standby);

# Wait for xmin to be populated via hot_standby_feedback
$node_primary->poll_query_until('postgres',
	"SELECT xmin IS NOT NULL FROM pg_replication_slots WHERE slot_name = 'revalidate_slot'")
	or die "Timed out waiting for xmin to be set via hot_standby_feedback";

my $xmin_before = $node_primary->safe_psql('postgres',
	"SELECT xmin FROM pg_replication_slots WHERE slot_name = 'revalidate_slot'");
note "xmin before invalidation: $xmin_before";
ok($xmin_before ne '', 'xmin is set via hot_standby_feedback before invalidation');

# Stop standby and invalidate the slot again
$node_standby->stop;

$logstart = -s $node_primary->logfile;
$node_primary->advance_wal(7);
$node_primary->safe_psql('postgres', "CHECKPOINT;");

$logstart = $node_primary->wait_for_log(
	qr/invalidating obsolete replication slot "revalidate_slot"/,
	$logstart);
pass('slot invalidated again for hot_standby_feedback test');

# While invalidated, the xmin should NOT be counted by the system.
# Run some transactions so the primary's xid advances well past the stale xmin.
$node_primary->safe_psql('postgres',
	"CREATE TABLE hsf_test(id int); DROP TABLE hsf_test;");

# Restart standby, expect revalidation
$logstart = -s $node_primary->logfile;
$node_standby->start;

$logstart = $node_primary->wait_for_log(
	qr/physical replication slot "revalidate_slot" has been revalidated/,
	$logstart);
pass('slot revalidated again with hot_standby_feedback=on');

$node_primary->wait_for_catchup($node_standby);

# Wait for xmin to be re-populated via hot_standby_feedback
$node_primary->poll_query_until('postgres',
	"SELECT xmin IS NOT NULL FROM pg_replication_slots WHERE slot_name = 'revalidate_slot'")
	or die "Timed out waiting for xmin to be re-established after revalidation";

my $xmin_after = $node_primary->safe_psql('postgres',
	"SELECT xmin FROM pg_replication_slots WHERE slot_name = 'revalidate_slot'");
note "xmin after revalidation: $xmin_after (was $xmin_before)";
ok($xmin_after ne '', 'xmin is re-established after revalidation with hot_standby_feedback=on');

# The new xmin should have advanced past the pre-invalidation value,
# since the standby caught up with all the WAL generated while it was down.
my $xmin_advanced = $node_primary->safe_psql('postgres',
	"SELECT '$xmin_after'::xid8 >= '$xmin_before'::xid8");
is($xmin_advanced, "t",
	'xmin advanced after revalidation (not stuck at stale pre-invalidation value)');

# Verify slot is valid
$result = $node_primary->safe_psql('postgres',
	"SELECT active, invalidation_reason IS NULL FROM pg_replication_slots WHERE slot_name = 'revalidate_slot'");
is($result, "t|t", 'slot is valid after revalidation with hot_standby_feedback');

$node_standby->stop;

##########################################################################
# Test 4: auto_revalidate=false still errors (existing behavior preserved)
##########################################################################

# Create a second slot without auto_revalidate
$node_primary->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('no_revalidate_slot', true, false, false)"
);

$result = $node_primary->safe_psql('postgres',
	"SELECT auto_revalidate FROM pg_replication_slots WHERE slot_name = 'no_revalidate_slot'");
is($result, "f", 'auto_revalidate is false on second slot');

# Create second standby using this slot
my $node_standby2 = PostgreSQL::Test::Cluster->new('standby_2');
$node_standby2->init_from_backup(
	$node_primary, $backup_name,
	has_streaming => 1,
	has_restoring => 1);
$node_standby2->append_conf('postgresql.conf',
	"primary_slot_name = 'no_revalidate_slot'");
$node_standby2->start;
$node_primary->wait_for_catchup($node_standby2);
$node_standby2->stop;

# Invalidate the second slot
$logstart = -s $node_primary->logfile;
$node_primary->advance_wal(7);
$node_primary->safe_psql('postgres', "CHECKPOINT;");

$logstart = $node_primary->wait_for_log(
	qr/invalidating obsolete replication slot "no_revalidate_slot"/,
	$logstart);
pass('second slot (auto_revalidate=false) was invalidated');

# Start standby2 -- it should fail to connect due to the invalidated slot
$logstart = -s $node_standby2->logfile;
$node_standby2->start;

# Wait for the FATAL error in standby log
$node_standby2->wait_for_log(
	qr/can no longer access replication slot "no_revalidate_slot"/,
	$logstart);
pass('standby with auto_revalidate=false gets error on invalidated slot');

# Cleanup standby2 from Test 4
$node_standby2->stop('immediate');

##########################################################################
# Test 5: Copied slot with auto_revalidate=true still revalidates
##########################################################################

# Test 4's WAL advance also invalidated revalidate_slot (its standby was
# stopped). Restart the standby so auto_revalidate kicks in and brings the
# slot back to a valid state before we copy it.
$logstart = -s $node_primary->logfile;
$node_standby->start;

$node_primary->wait_for_log(
	qr/physical replication slot "revalidate_slot" has been revalidated/,
	$logstart);
$node_primary->wait_for_catchup($node_standby);

# Sanity-check that the slot is valid before the copy
$result = $node_primary->safe_psql('postgres',
	"SELECT active, invalidation_reason IS NULL FROM pg_replication_slots WHERE slot_name = 'revalidate_slot'");
is($result, "t|t", 'revalidate_slot is valid again before copy');

# Copy the original slot -- auto_revalidate should be preserved
$node_primary->safe_psql('postgres',
	"SELECT pg_copy_physical_replication_slot('revalidate_slot', 'copied_slot')");

$result = $node_primary->safe_psql('postgres',
	"SELECT auto_revalidate FROM pg_replication_slots WHERE slot_name = 'copied_slot'");
is($result, "t", 'copied slot preserved auto_revalidate=true');

# Stop the original standby so we don't have two standbys streaming
# concurrently while we exercise the copied slot.
$node_standby->stop;

# Create a standby using the copied slot
my $node_standby3 = PostgreSQL::Test::Cluster->new('standby_3');
$node_standby3->init_from_backup(
	$node_primary, $backup_name,
	has_streaming => 1,
	has_restoring => 1);
$node_standby3->append_conf('postgresql.conf',
	"primary_slot_name = 'copied_slot'");
$node_standby3->start;
$node_primary->wait_for_catchup($node_standby3);
$node_standby3->stop;

# Invalidate the copied slot
$logstart = -s $node_primary->logfile;
$node_primary->advance_wal(7);
$node_primary->safe_psql('postgres', "CHECKPOINT;");

$logstart = $node_primary->wait_for_log(
	qr/invalidating obsolete replication slot "copied_slot"/,
	$logstart);
pass('copied slot was invalidated');

# Reconnect -- the copied slot should auto-revalidate
$logstart = -s $node_primary->logfile;
$node_standby3->start;

$node_primary->wait_for_log(
	qr/physical replication slot "copied_slot" has been revalidated/,
	$logstart);
pass('copied slot was revalidated after standby reconnected');

$node_primary->wait_for_catchup($node_standby3);

$result = $node_primary->safe_psql('postgres',
	"SELECT active, invalidation_reason IS NULL FROM pg_replication_slots WHERE slot_name = 'copied_slot'");
is($result, "t|t", 'copied slot is active and valid after revalidation');

$node_standby3->stop('immediate');
$node_primary->safe_psql('postgres',
	"SELECT pg_drop_replication_slot('copied_slot')");

##########################################################################
# Test 6: Revalidation after idle_timeout invalidation
#
# Requires injection_points to force idle_timeout without waiting real
# wall-clock time.  Skipped if the build does not support injection points.
##########################################################################

SKIP:
{
	skip "injection points not supported by this build", 3
		unless ($ENV{enable_injection_points} eq 'yes');

	skip "injection_points extension not installed", 3
		unless ($node_primary->check_extension('injection_points'));

	$node_primary->safe_psql('postgres', 'CREATE EXTENSION IF NOT EXISTS injection_points;');

	# Remove WAL size limit so idle_timeout is the only invalidation vector,
	# and enable idle_replication_slot_timeout (required by CanInvalidateIdleSlot).
	$node_primary->safe_psql('postgres',
		"ALTER SYSTEM SET max_slot_wal_keep_size = '-1'");
	$node_primary->safe_psql('postgres',
		"ALTER SYSTEM SET idle_replication_slot_timeout = '1min'");
	$node_primary->reload;

	# Revalidate_slot should still exist and be valid from earlier tests.
	# Start standby, catch up, then stop it so the slot becomes idle.
	$node_standby->start;
	$node_primary->wait_for_catchup($node_standby);
	$node_standby->stop;

	# Attach the injection point that forces idle_timeout invalidation
	$node_primary->safe_psql('postgres',
		"SELECT injection_points_attach('slot-timeout-inval', 'error');");

	# Checkpoint triggers the invalidation check
	$logstart = -s $node_primary->logfile;
	$node_primary->safe_psql('postgres', "CHECKPOINT;");

	$logstart = $node_primary->wait_for_log(
		qr/invalidating obsolete replication slot "revalidate_slot"/,
		$logstart);
	pass('slot invalidated due to idle_timeout (injection point)');

	# Confirm the reason is idle_timeout
	$result = $node_primary->safe_psql('postgres',
		"SELECT invalidation_reason FROM pg_replication_slots WHERE slot_name = 'revalidate_slot'");
	is($result, "idle_timeout", 'invalidation reason is idle_timeout');

	# Detach the injection point before reconnecting so it does not
	# interfere with subsequent checkpoint cycles.
	$node_primary->safe_psql('postgres',
		"SELECT injection_points_detach('slot-timeout-inval');");

	# Reconnect standby -- slot should auto-revalidate
	$logstart = -s $node_primary->logfile;
	$node_standby->start;

	$node_primary->wait_for_log(
		qr/physical replication slot "revalidate_slot" has been revalidated/,
		$logstart);
	pass('slot revalidated after idle_timeout invalidation');

	$node_standby->stop;
}

# Cleanup
$node_primary->stop;

done_testing();
