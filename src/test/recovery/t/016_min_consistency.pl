# Test for checking consistency of on-disk pages for a cluster with
# the minimum recovery LSN, ensuring that the updates happen across
# all processes.  In this test, the updates from the startup process
# and the checkpointer (which triggers non-startup code paths) are
# both checked.

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 2;

# Find the largest LSN in the set of pages part of the given relation
# file.  This is used for offline checks of page consistency.  The LSN
# is historically stored as a set of two numbers of 4 byte-length
# located at the beginning of each page.
sub find_largest_lsn
{
	my $blocksize = int(shift);
	my $filename = shift;
	my ($max_hi,$max_lo) = (0,0);
	open(my $fh, "<:raw", $filename)
	  or die "failed to open $filename: $!";
	my ($buf,$len);
	while ($len = read($fh, $buf, $blocksize))
	{
		$len == $blocksize
		  or die "read only $len of $blocksize bytes from $filename";
		my ($hi,$lo) = unpack("LL", $buf);

		if ($hi > $max_hi or ($hi == $max_hi and $lo > $max_lo))
		{
			($max_hi,$max_lo) = ($hi,$lo);
		}
	}
	defined($len) or die "read error on $filename: $!";
	close($fh);

	return sprintf("%X/%X", $max_hi, $max_lo);
}

# Initialize primary node
my $primary = get_new_node('primary');
$primary->init(allows_streaming => 1);

# Set shared_buffers to a very low value to enforce discard and flush
# of PostgreSQL buffers on standby, enforcing other processes than the
# startup process to update the minimum recovery LSN in the control
# file.  Autovacuum is disabled so as there is no risk of having other
# processes than the checkpointer doing page flushes.
$primary->append_conf("postgresql.conf", <<EOF);
shared_buffers = 128kB
autovacuum = off
EOF

# Start the primary
$primary->start;

# setup/start a standby
$primary->backup('bkp');
my $standby = get_new_node('standby');
$standby->init_from_backup($primary, 'bkp', has_streaming => 1);
$standby->start;

# Object creations for the upcoming tests:
# - Base table whose data consistency is checked.
# - pageinspect to look at the page-level contents.
# - Function wrapper on top of pageinspect to scan a range of pages and
#   get the maximum LSN present.
$primary->safe_psql('postgres', "
create extension pageinspect;
-- Function wrapper on top of pageinspect which fetches the largest LSN
-- present in the given page range.
create or replace function max_lsn_range(relname text,
  start_blk int,
  end_blk int)
returns pg_lsn as \$\$
declare
  max_lsn pg_lsn = '0/0'::pg_lsn;
  cur_lsn pg_lsn;
begin
  for i in start_blk..end_blk loop
    execute 'select lsn from page_header(get_raw_page(''' || relname || ''',' || i || '));' into cur_lsn;
    if max_lsn < cur_lsn then
      max_lsn = cur_lsn;
    end if;
  end loop;
  return max_lsn;
end;
\$\$ language plpgsql;
create table test1 (a int) with (fillfactor = 10);
insert into test1 select generate_series(1, 10000);");

# Take a checkpoint and enforce post-checkpoint full page writes
# which makes the startup process replay those pages, updating
# minRecoveryPoint.
$primary->safe_psql('postgres', 'checkpoint');
$primary->safe_psql('postgres', 'update test1 set a = a + 1');

# Fill in the standby's shared buffers with the data filled in
# previously.
$standby->safe_psql('postgres', 'select count(*) from test1');

# Update the table again, this does not generate full page writes so
# the standby will replay records associated with it, but the startup
# process will not flush those pages.
$primary->safe_psql('postgres', 'update test1 set a = a + 1');

# Extract from the relation the last block created and its relation
# file, this will be used at the end of the test for sanity checks.
my $blocksize = $primary->safe_psql('postgres',
	"select setting::int from pg_settings where name = 'block_size';");
my $last_block = $primary->safe_psql('postgres',
	"select pg_relation_size('test1')::int / $blocksize - 1");
my $relfilenode = $primary->safe_psql('postgres',
	"select pg_relation_filepath('test1'::regclass);");

# Wait for last record to have been replayed on the standby.
$primary->wait_for_catchup($standby, 'replay',
						   $primary->lsn('insert'));

# Issue a restart point on the standby now, which makes the checkpointer
# update minRecoveryPoint.
$standby->safe_psql('postgres', 'checkpoint');

# Now shut down the primary violently so as the standby does not
# receive the shutdown checkpoint, making sure that the startup
# process does not flush any pages on its side.  The standby is
# cleanly stopped, which makes the checkpointer update minRecoveryPoint
# with the restart point created at shutdown.
$primary->stop('immediate');
$standby->stop('fast');

# Check the data consistency of the instance while offline.  This is
# done by directly scanning the on-disk relation blocks and what
# pg_controldata lets know.
my $standby_data = $standby->data_dir;
my $offline_max_lsn = find_largest_lsn($blocksize,
									   "$standby_data/$relfilenode");

# Fetch minRecoveryPoint from the control file itself
my ($stdout, $stderr) = run_command(['pg_controldata', $standby_data]);
my @control_data = split("\n", $stdout);
my $offline_recovery_lsn = undef;
foreach (@control_data)
{
	if ($_ =~ /^Minimum recovery ending location:\s*(.*)$/mg)
	{
		$offline_recovery_lsn = $1;
		last;
	}
}
die "No minRecoveryPoint in control file found\n"
	unless defined($offline_recovery_lsn);

# minRecoveryPoint should never be
ok($offline_recovery_lsn ge $offline_max_lsn,
   "Check offline that table data is consistent with minRecoveryPoint");

# Now restart the standby and check the state of the instance while
# online.  Again, all the pages of the relation previously created
# should not have a LSN newer than what minRecoveryPoint has.
$standby->start;

# Check that the last page of the table, which is the last one which
# has been flushed by the previous checkpoint on the standby, does not
# have a LSN newer than minRecoveryPoint.
my $psql_out;
$standby->psql(
	'postgres',
	"SELECT max_lsn_range('test1', 0, $last_block) <= min_recovery_end_lsn from pg_control_recovery()",
	stdout => \$psql_out);
is($psql_out, 't',
   "Check online that table data is consistent with minRecoveryPoint");
