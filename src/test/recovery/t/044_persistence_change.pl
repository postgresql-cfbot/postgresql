# Copyright (c) 2023-2024, PostgreSQL Global Development Group
#
# Test in-place relation persistence changes

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use Test::More;

my @relnames = ('t', 'i_bt', 'i_gin', 'i_gist', 'i_hash', 'i_brin', 'i_spgist');
my @noninplace_names = ('i_gist');

# This feature works differently by wal_level.
run_test('minimal');
run_test('replica');
done_testing();

sub run_test
{
	my ($wal_level) = @_;

	note "## run with wal_level = $wal_level";

	# Initialize primary node.
	my $node = PostgreSQL::Test::Cluster->new("node_$wal_level");
	$node->init;
	# Inhibit checkpoints to run
	$node->append_conf('postgresql.conf', qq(
wal_level = $wal_level
checkpoint_timeout = '24h'
max_prepared_transactions = 2
	));
	$node->start;

	my $datadir = $node->data_dir;
	my $datoid = $node->safe_psql('postgres',
	  q/SELECT oid FROM pg_database WHERE datname = current_database()/);
	my $dbdir = $node->data_dir . "/base/$datoid";

	# Create a table and indexes of built-in kinds
	$node->psql('postgres',	qq(
	CREATE TABLE t (bt int, gin int[], gist point, hash	int,
					brin int, spgist point);
	CREATE INDEX i_bt ON t USING btree (bt);
	CREATE INDEX i_gin ON t USING gin (gin);
	CREATE INDEX i_gist ON t USING gist (gist);
	CREATE INDEX i_hash ON t USING hash (hash);
	CREATE INDEX i_brin ON t USING brin (brin);
	CREATE INDEX i_spgist ON t USING spgist (spgist);));

	my $relfilenodes1 = getrelfilenodes($node, \@relnames);

	# the number must correspond to the in list above
	is (scalar %{$relfilenodes1}, 7, "number of relations is correct");

	# check initial state
	ok (check_storage_state(\&is_logged_state, $node, \@relnames),
		"storages are in logged state");

	# Normal crash-recovery of LOGGED tables
	$node->stop('immediate');
	$node->start;

	# Insert data 0 to 1999
	$node->psql('postgres', insert_data_query(0, 2000));

	# Check if the data survives a crash
	$node->stop('immediate');
	$node->start;
	is ($node->safe_psql('postgres', "SELECT count(*) FROM t;"), 2000,
		"data loss check: crash with LOGGED table");

	# Change the table to UNLOGGED then commit.
	$node->psql('postgres', 'ALTER TABLE t SET UNLOGGED');

	# Check if SET UNLOGGED above didn't change relfilenumbers.
	my $relfilenodes2 = getrelfilenodes($node, \@relnames);
	ok (checkrelfilenodes($relfilenodes1, $relfilenodes2),
		"relfilenumber transition is as expected after SET UNLOGGED");

	# check init-file state
	ok (check_storage_state(\&is_unlogged_state, $node, \@relnames),
		"storages are in unlogged state");

	# Check if the table is reset through recovery.
	$node->stop('immediate');
	$node->start;
	is ($node->safe_psql('postgres', "SELECT count(*) FROM t;"), 0,
		"table data is reset though recovery");

	# check reset state
	ok (check_storage_state(\&is_reset_state, $node, \@relnames),
		"storages are in reset state");

	# Insert data 0 to 1999, then set persistence to LOGGED then crash.
	$node->psql('postgres', insert_data_query(0, 2000));
	$node->psql('postgres', qq(ALTER TABLE t SET LOGGED));
	$node->stop('immediate');
	$node->start;

	# Check if SET LOGGED didn't change relfilenumbers and data survive a crash
	my $relfilenodes3 = getrelfilenodes($node, \@relnames);
	ok (!checkrelfilenodes($relfilenodes2, $relfilenodes3),
		"crashed SET-LOGGED relations have sane relfilenodes transition");

	is ($node->safe_psql('postgres', "SELECT count(*) FROM t;"), 2000,
		"crashed SET-LOGGED table does not lose data");

	# Change to UNLOGGED then insert data, then shutdown normally.
	$node->psql('postgres', 'ALTER TABLE t SET UNLOGGED');
	$node->psql('postgres', insert_data_query(2000, 2000)); # 2000 - 3999
	$node->stop;
	$node->start;
	is ($node->safe_psql('postgres', "SELECT count(*) FROM t;"), 4000,
		"UNLOGGED table does not lose data after graceful restart");

	# Test for mid-transaction change to LOGGED and crash.
	# Now, the table has data 0-3999
	$node->psql('postgres', insert_data_query(4000, 2000)); # 4000 - 5999

	my $sess = $node->interactive_psql('postgres');
	$sess->set_query_timer_restart();
	$sess->query('BEGIN; ALTER TABLE t SET LOGGED');
	$sess->query(insert_data_query(6000, 2000)); # 6000-7999, no commit
	$node->stop('immediate');
	$sess->quit;
	$node->start;
	is ($node->safe_psql('postgres', "SELECT count(*) FROM t;"), 0,
		"table is reset after in-transaction SET-LOGGED then insert");
	ok (check_storage_state(\&is_unlogged_state, $node, \@relnames),
		"storages are reverted to unlogged state");

	# Test for mid-transaction change to UNLOGGED and crash.
	# Now, the table has no data
	$node->psql('postgres', 'ALTER TABLE t SET LOGGED');
	$node->psql('postgres', insert_data_query(0, 2000)); # 0 - 1999
	$sess = $node->interactive_psql('postgres');
	$sess->set_query_timer_restart();
	$sess->query('BEGIN; ALTER TABLE t SET UNLOGGED');
	$sess->query(insert_data_query(2000, 2000)); # 2000-3999, no commit
	$node->stop('immediate');
	$sess->quit;
	$node->start;
	is ($node->safe_psql('postgres', "SELECT count(*) FROM t;"), 2000,
		"table is reset after in-transaction SET-UNLOGGED then insert");
	ok (check_storage_state(\&is_logged_state, $node, \@relnames),
		"storages are reverted to logged state");

	### Subtransactions
	ok ($node->psql('postgres',
				qq(
				BEGIN;
				ALTER TABLE t SET UNLOGGED;	-- committed
				SAVEPOINT a;
				ALTER TABLE t SET LOGGED;   -- aborted
				SAVEPOINT b;
				ROLLBACK TO a;
				COMMIT;
				)) != 3,
	   "command succeeds 1");

	is ($node->safe_psql('postgres', "SELECT count(*) FROM t;"), 2000,
	   "table data is not changed 1");
	ok (check_storage_state(\&is_unlogged_state, $node, \@relnames),
		"storages are changed to unlogged state");

	ok ($node->psql('postgres',
				qq(
				BEGIN;
				ALTER TABLE t SET LOGGED;	-- aborted
				SAVEPOINT a;
				ALTER TABLE t SET UNLOGGED;   -- aborted
				SAVEPOINT b;
				RELEASE a;
				ROLLBACK;
				)) != 3,
		"command succeeds 2");

	is ($node->safe_psql('postgres', "SELECT count(*) FROM t;"), 2000,
		"table data is not changed 2");
	ok (check_storage_state(\&is_unlogged_state, $node, \@relnames),
		"storages stay in unlogged state");

	### Prepared transactions
	my ($ret, $stdout, $stderr) =
	  $node->psql('postgres',
				  qq(
					ALTER TABLE t SET LOGGED;
					BEGIN;
					ALTER TABLE t SET UNLOGGED;
					PREPARE TRANSACTION 'a';
					COMMIT PREPARED 'a';
					));
	ok ($stderr =~ m/cannot prepare transaction if persistence change/,
		"errors out when persistence-flipped xact is prepared");
	ok (check_storage_state(\&is_logged_state, $node, \@relnames),
		"storages are in logged state");

	($ret, $stdout, $stderr) =
	  $node->psql('postgres',
				qq(
				BEGIN;
				SAVEPOINT a;
				ALTER TABLE t SET UNLOGGED;
				PREPARE TRANSACTION 'a';
				ROLLBACK PREPARED 'a';
				));
	ok ($stderr =~ m/cannot prepare transaction if persistence change/,
		"errors out when persistence-flipped xact is prepared 2");
	ok (check_storage_state(\&is_logged_state, $node, \@relnames),
		"storages stay in logged state");

	### Error out DML
	$node->psql('postgres',
				qq(
					BEGIN;
				  	ALTER TABLE t SET LOGGED;
				  	INSERT INTO t VALUES(1); -- Succeeds
				  	COMMIT;
		  		  	));

	($ret, $stdout, $stderr) =
	  $node->psql('postgres',
				  qq(
				  BEGIN;
				  ALTER TABLE t SET UNLOGGED;
				  INSERT INTO t VALUES(2); -- ERROR
				  ));
	ok ($stderr =~ m/cannot execute INSERT on relation/,
	   "errors out when DML is issued after persistence toggling");

	ok ($node->psql('postgres',
				qq(
				BEGIN;
				SAVEPOINT a;
	  			ALTER TABLE t SET UNLOGGED;
				ROLLBACK TO a;
				INSERT INTO t VALUES(3); -- Succeeds
				COMMIT;
	  		  	)) != 3,
		"insert after rolled-back persistence change succeeds");

	($ret, $stdout, $stderr) =
	  $node->psql('postgres',
				  qq(
				  BEGIN;
				  SAVEPOINT a;
				  ALTER TABLE t SET UNLOGGED;
				  RELEASE a;
				  UPDATE t SET bt = bt + 1; -- ERROR
		  		  ));
	ok ($stderr =~ m/cannot execute UPDATE on relation/,
		"errors out when DML is issued after persistence toggling in subxact");
	
$node->stop;
	$node->teardown_node;
}

#==== helper routines

# Generates a query to insert data from $st to $st + $num - 1
sub insert_data_query
{
	my ($st, $num) = @_;
	my $ed = $st + $num - 1;
	my $query = qq(
INSERT INTO t
 (SELECT i, ARRAY[i, i * 2], point(i, i * 2), i, i, point(i, i)
  FROM generate_series($st, $ed) i);
);
	return $query;
}

sub check_indexes
{
	my ($node, $st, $ed) = @_;
	my $num_data = $ed - $st;

	is ($node->safe_psql('postgres', qq(
			SET enable_seqscan TO true;
			SET enable_indexscan TO false;
			SELECT COUNT(*) FROM t, generate_series($st, $ed) i
			WHERE bt = i)),
	   $num_data, "heap is not broken");
	is ($node->safe_psql('postgres', qq(
			SET enable_seqscan TO false;
			SET enable_indexscan TO true;
			SELECT COUNT(*) FROM t, generate_series($st, $ed) i
			WHERE bt = i)),
	   $num_data, "btree is not broken");
	is ($node->safe_psql('postgres', qq(
			SET enable_seqscan TO false;
			SET enable_indexscan TO true;
			SELECT COUNT(*) FROM t, generate_series($st, $ed) i
			WHERE gin = ARRAY[i, i * 2];)),
	   $num_data, "gin is not broken");
	is ($node->safe_psql('postgres', qq(
			SET enable_seqscan TO false;
			SET enable_indexscan TO true;
			SELECT COUNT(*) FROM t, generate_series($st, $ed) i
			WHERE gist <@ box(point(i-0.5, i*2-0.5),point(i+0.5, i*2+0.5));)),
	   $num_data, "gist is not broken");
	is ($node->safe_psql('postgres', qq(
			SET enable_seqscan TO false;
			SET enable_indexscan TO true;
			SELECT COUNT(*) FROM t, generate_series($st, $ed) i
			WHERE hash = i;)),
	   $num_data, "hash is not broken");
	is ($node->safe_psql('postgres', qq(
			SET enable_seqscan TO false;
			SET enable_indexscan TO true;
			SELECT COUNT(*) FROM t, generate_series($st, $ed) i
			WHERE brin = i;)),
	   $num_data, "brin is not broken");
	is ($node->safe_psql('postgres', qq(
			SET enable_seqscan TO false;
			SET enable_indexscan TO true;
			SELECT COUNT(*) FROM t, generate_series($st, $ed) i
			WHERE spgist <@ box(point(i-0.5,i-0.5),point(i+0.5,i+0.5));)),
	   $num_data, "spgist is not broken");
}

sub getrelfilenodes
{
	my ($node, $relnames) = @_;

	my $result = $node->safe_psql('postgres',
		'SELECT relname, relfilenode FROM pg_class
		 WHERE relname
		 IN (\'' .
		join("','", @{$relnames}).						 
		'\') ORDER BY oid');

	my %relfilenodes;

	foreach my $l (split(/\n/, $result))
	{
		die "unexpected format: $l" if ($l !~ /^([^|]+)\|([0-9]+)$/);
		$relfilenodes{$1} = $2;
	}

	return \%relfilenodes;
}

sub checkrelfilenodes
{
	my ($rnodes1, $rnodes2) = @_;
	my $result = 1;

	foreach my $n (keys %{$rnodes1})
	{
		if (grep { $n eq $_ } @noninplace_names)
		{
			if ($rnodes1->{$n} == $rnodes2->{$n})
			{
				$result = 0;
				note sprintf("$n: relfilenode is not changed: %d",
							 $rnodes1->{$n});
			}
		}
		else
		{
			if ($rnodes1->{$n} != $rnodes2->{$n})
			{
				$result = 0;
				note sprintf("$n: relfilenode is changed: %d => %d",
							 $rnodes1->{$n}, $rnodes2->{$n});
			}
		}
	}
	return $result;
}

sub getfilenames
{
	my ($dirname) = @_;

	my $dir = opendir(my $dh, $dirname) or die "could not open $dirname: $!";
	my @f = readdir($dh);
	closedir($dh);

	my @result = grep {$_ !~ /^..?$/} @f;

	return \@result;
}

sub init_fork_exists
{
	my ($relfilenodes, $datafiles, $relname) = @_;

	my $relfnumber = ${$relfilenodes}{$relname};
	my $init_exists = grep {/^${relfnumber}_init$/} @{$datafiles};

	return $init_exists;
}

sub noninit_forks_exist
{
	my ($relfilenodes, $datafiles, $relname) = @_;

	my $relfnumber = ${$relfilenodes}{$relname};
	my $noninit_exists = grep {/^${relfnumber}(_(?!init).*)?$/} @{$datafiles};

	return $noninit_exists;
}

sub is_logged_state
{
	my ($node, $relfilenodes, $datafiles, $relname) = @_;

	my $relfnumber = ${$relfilenodes}{$relname};
	my $init_exists = grep {/^${relfnumber}_init$/} @{$datafiles};
	my $main_exists = grep {/^${relfnumber}$/} @{$datafiles};
	my $persistence = $node->safe_psql('postgres',
	   qq(
		SELECT relpersistence FROM pg_class WHERE relname = '$relname'
		));

	if ($init_exists || !$main_exists || $persistence ne 'p')
	{
		# note the state if this test failed
		note "## is_logged_state:($relname): \$init_exists=$init_exists, \$main_exists=$main_exists, \$persistence='$persistence'\n";
		return 0 ;
	}

	return 1;
}
  
sub is_unlogged_state
{
	my ($node, $relfilenodes, $datafiles, $relname) = @_;

	my $relfnumber = ${$relfilenodes}{$relname};
	my $init_exists = grep {/^${relfnumber}_init$/} @{$datafiles};
	my $main_exists = grep {/^${relfnumber}$/} @{$datafiles};
	my $persistence = $node->safe_psql('postgres',
	   qq(
		SELECT relpersistence FROM pg_class WHERE relname = '$relname'
		));

	if (!$init_exists || !$main_exists || $persistence ne 'u')
	{
		# note the state if this test failed
		note "is_unlogged_state:($relname): \$init_exists=$init_exists, \$main_exists=$main_exists, \$persistence='$persistence'\n";
		return 0 ;
	}

	return 1;
}
  
sub is_reset_state
{
	my ($node, $relfilenodes, $datafiles, $relname) = @_;

	my $datoid = $node->safe_psql('postgres',
	  q/SELECT oid FROM pg_database WHERE datname = current_database()/);
	my $dbdir = $node->data_dir . "/base/$datoid";
	my $relfnumber = ${$relfilenodes}{$relname};
	my $init_exists = grep {/^${relfnumber}_init$/} @{$datafiles};
	my $main_exists = grep {/^${relfnumber}$/} @{$datafiles};
	my $others_not_exist = !grep {/^${relfnumber}_(?!init).*$/} @{$datafiles};
	my $persistence = $node->safe_psql('postgres',
	   qq(
		SELECT relpersistence FROM pg_class WHERE relname = '$relname'
		));

	if (!$init_exists || !$main_exists || !$others_not_exist ||
	  $persistence ne 'u')
	{
		# note the state if this test failed
		note "## is_reset_state:($relname): \$init_exists=$init_exists, \$main_exists=$main_exists, \$others_not_exist=$others_not_exist, \$persistence='$persistence'\n";
		return 0 ;
	}

	my $main_file = "$dbdir/${relfnumber}";
	my $init_file = "$dbdir/${relfnumber}_init";
	my $main_file_size = -s $main_file;
	my $init_file_size = -s $init_file;

	if ($main_file_size != $init_file_size)
	{
		note "## is_reset_state:($relname): \$main_file='$main_file', size=$main_file_size, \$init_file='$init_file', size=$init_file_size\n";
		return 0;
	}

	return 1;
}

sub check_storage_state
{
	my ($func, $node, $relnames) = @_;
	my $relfilenodes = getrelfilenodes($node, $relnames);
	my $datoid = $node->safe_psql('postgres',
	  q/SELECT oid FROM pg_database WHERE datname = current_database()/);
	my $dbdir = $node->data_dir . "/base/$datoid";
	my $datafiles = getfilenames($dbdir);
	my $result = 1;
	
	foreach my $relname (@{$relnames})
	{
		if (!$func->($node, $relfilenodes, $datafiles, $relname))
		{
			$result = 0;

			## do not return immediately, run this test for all
			## relations to leave diagnosis information in the log
			## file.
		}
	}

	return $result;
}
