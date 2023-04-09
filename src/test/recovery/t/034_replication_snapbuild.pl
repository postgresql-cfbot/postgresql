# Copyright (c) 2021-2022, PostgreSQL Global Development Group

# Test replication snapshot rebuild.
use strict;
use warnings;
use TestLib;
use PostgresNode;
use Test::More;
use DBI;
use Test::More tests => 1;

# set slot number
my $slot_num = 100;

# initialize master node
my $master = get_new_node('master');
$master->init;
$master->append_conf(
	'postgresql.conf', qq(
wal_level=logical
max_connections=1000
max_worker_processes=500
max_wal_senders=500
max_replication_slots=500
max_logical_replication_workers=500
log_error_verbosity = verbose
log_statement = all
));
$master->start;
my $master_port = $master->port;

# master init
for (my $i = 0; $i<$slot_num; $i = $i+1)
{
	my $table_name=qq(t_$i);
	my $pub_name=qq(pub_$i);
	$master->safe_psql('postgres', "create table $table_name(a int);");
	$master->safe_psql('postgres', "create publication $pub_name for table $table_name;");
}

# create subscription
my $subscriber = get_new_node('subscriber');
$subscriber->init;
$subscriber->append_conf(
	'postgresql.conf', qq(
wal_level=logical
max_connections=1000
max_worker_processes=500
max_replication_slots=500
log_error_verbosity = verbose
max_logical_replication_workers=500
log_statement = all
));
$subscriber->start;

# subscriber init
for (my $i = 0; $i<$slot_num; $i = $i+1)
{
	my $table_name=qq(t_$i);
	my $pub_name=qq(pub_$i);
	my $sub_name=qq(sub_$i);

	$subscriber->safe_psql('postgres',
	"create table $table_name(a int);");
}

my $script = $master->basedir . '/pgbench_script';
for (my $i = 0; $i<$slot_num; $i = $i+1)
{
	my $table_name=qq(t_$i);

	append_to_file($script,
	"INSERT INTO $table_name SELECT FROM generate_series(1,100);");
}

my $pid = fork();
if ($pid == 0)
{
	sleep(1);
	for (my $i = 0; $i<$slot_num; $i = $i+1)
	{
		my $pub_name=qq(pub_$i);
		my $sub_name=qq(sub_$i);

		$subscriber->safe_psql('postgres',
		"create subscription $sub_name connection 'port=$master_port dbname=postgres user=postgres' publication $pub_name;");
	}
	exec "echo", "\nbye\n";
}
elsif ($pid < 0)
{
	die "failed to fork new process";
}

$master->run_log(
	[qw(pgbench --no-vacuum --client=10 -U postgres
	-T 25 --file), $script ]);

my $result = $master->safe_psql('postgres', "select count(*) from pg_replication_slots;");
is($result, qq($slot_num), "Create slots successfully.")