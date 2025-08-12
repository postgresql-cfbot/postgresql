# Test logical replication with publish_via_partition_root,
# where the parent has REPLICA IDENTITY FULL, but one partition does not.
#
# Expected behavior:
# - For partitions with REPLICA IDENTITY FULL, old tuple must be marked as 'O' and contain full row.
# - For partitions with REPLICA IDENTITY DEFAULT, old tuple should be marked as 'K' and contain only key columns.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

sub log_state
{
  my ($node, $label) = @_;

  my $rows = $node->safe_psql('postgres', q{
    SELECT tableoid::regclass AS part,
           id,
           to_char(ts,'YYYY-MM-DD') AS ts,
           load
    FROM   part_table
    ORDER  BY 1,2;
  });
  diag "----- $label: rows -----\n$rows\n";
}

my $pub = PostgreSQL::Test::Cluster->new('publisher');
$pub->init(allows_streaming => 'logical');
$pub->start;

my $sub = PostgreSQL::Test::Cluster->new('subscriber');
$sub->init;
$sub->start;

$pub->safe_psql('postgres', q{
create table part_table(
  id  int generated always as identity,
  ts  timestamp,
  load text,
  constraint part_table_pk primary key(id, ts)
) partition by range(ts);

create table part_table_sect_1 partition of part_table
  for values from ('2000-01-01') to ('2024-01-01');
create table part_table_sect_2 partition of part_table
  for values from ('2024-01-01') to (maxvalue);

alter table part_table        replica identity full;
alter table part_table_sect_1 replica identity full;

create publication pub_part_table
  for table part_table
  with (publish_via_partition_root = true);
});

$pub->safe_psql('postgres',
  q{select pg_create_logical_replication_slot('slot_test', 'pgoutput');});

$sub->safe_psql('postgres', q{
create table part_table(
  id  int,
  ts  timestamp,
  load text,
  constraint part_table_pk primary key(id, ts)
) partition by range(ts);

create table part_table_sect_1 partition of part_table
  for values from ('2000-01-01') to ('2024-01-01');
create table part_table_sect_2 partition of part_table
  for values from ('2024-01-01') to (maxvalue);
});

my $connstr = $pub->connstr . ' dbname=postgres';
$sub->safe_psql('postgres', qq{
create subscription sub_part
  connection '$connstr application_name=sub_part'
  publication pub_part_table;});

$sub->wait_for_subscription_sync($pub, 'sub_part');

$pub->safe_psql('postgres', q{
insert into part_table values (default, '2020-01-01 00:00', 'first');
insert into part_table values (default, '2025-01-01 00:00', 'second');
});
$sub->wait_for_subscription_sync($pub, 'sub_part');
diag("\n");
log_state($pub, 'publisher after insert');
log_state($sub, 'subscriber after insert');

$pub->safe_psql('postgres',
  q{update part_table set ts = ts + interval '1 day';});
$sub->wait_for_subscription_sync($pub, 'sub_part');

log_state($pub, 'publisher after update');
log_state($sub, 'subscriber after update');

$pub->safe_psql('postgres', q{delete from part_table;});
$sub->wait_for_subscription_sync($pub, 'sub_part');

log_state($pub, 'publisher after delete');
log_state($sub, 'subscriber after delete');

my $wal = $pub->safe_psql('postgres', q{
select string_agg(encode(data, 'escape'),'')
  from pg_logical_slot_get_binary_changes(
         'slot_test', null, null,
         'proto_version','1',
         'publication_names','pub_part_table');
});

diag("---- WAL stream ----\n$wal\n");

# 1: first partition has REPLICA IDENTITY FULL - full old tuple
# (see - https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-UPDATE)
like(
  $wal,
  qr/U.*O.*first.*first/s,
  'partition WITH REPLICA IDENTITY FULL contains full old tuple'
);

# 2: second partition has REPLICA IDENTITY DEFAULT - only keys expected.
if ($wal =~ /U.*K.*second/s)
{
    pass("Tag K correctly used for partition with REPLICA IDENTITY DEFAULT");
}
elsif ($wal =~ /(U.*O.*second)/s)
{
    my $blk = $1;
    my $count = () = $blk =~ /second/g;
    is($count, 2, "Tag O used but this partition with REPLICA IDENTITY DEFAULT");
}

done_testing();
