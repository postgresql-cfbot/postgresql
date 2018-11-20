# Test WAL replay for optimized TRUNCATE and COPY records
#
# WAL truncation is optimized in some cases with TRUNCATE and COPY queries
# which sometimes interact badly with the other optimizations in line with
# several setting values of wal_level, particularly when using "minimal" or
# "replica".  The optimization may be enabled or disabled depending on the
# scenarios dealt here, and should never result in any type of failures or
# data loss.
use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 14;

# Wrapper routine tunable for wal_level.
sub run_wal_optimize
{
	my $wal_level = shift;

	# Primary needs to have wal_level = minimal here
	my $node = get_new_node("node_$wal_level");
	$node->init;
	$node->append_conf('postgresql.conf', qq(
wal_level = $wal_level
));
	$node->start;

	# Test direct truncation optimization.  No tuples
	$node->safe_psql('postgres', "
		BEGIN;
		CREATE TABLE test1 (id serial PRIMARY KEY);
		TRUNCATE test1;
		COMMIT;");

	$node->stop('immediate');
	$node->start;

	my $result = $node->safe_psql('postgres', "SELECT count(*) FROM test1;");
	is($result, qq(0),
	   "wal_level = $wal_level, optimized truncation with empty table");

	# Test truncation with inserted tuples within the same transaction.
	# Tuples inserted after the truncation should be seen.
	$node->safe_psql('postgres', "
		BEGIN;
		CREATE TABLE test2 (id serial PRIMARY KEY);
		INSERT INTO test2 VALUES (DEFAULT);
		TRUNCATE test2;
		INSERT INTO test2 VALUES (DEFAULT);
		COMMIT;");

	$node->stop('immediate');
	$node->start;

	$result = $node->safe_psql('postgres', "SELECT count(*) FROM test2;");
	is($result, qq(1),
	   "wal_level = $wal_level, optimized truncation with inserted table");

	# Data file for COPY query in follow-up tests.
	my $basedir = $node->basedir;
	my $copy_file = "$basedir/copy_data.txt";
	TestLib::append_to_file($copy_file, qq(20000,30000
20001,30001
20002,30002));

	# Test truncation with inserted tuples using COPY.  Tuples copied after the
	# truncation should be seen.
	$node->safe_psql('postgres', "
		BEGIN;
		CREATE TABLE test3 (id serial PRIMARY KEY, id2 int);
		INSERT INTO test3 (id, id2) VALUES (DEFAULT, generate_series(1,10000));
		TRUNCATE test3;
		COPY test3 FROM '$copy_file' DELIMITER ',';
		COMMIT;");
	$node->stop('immediate');
	$node->start;
	$result = $node->safe_psql('postgres', "SELECT count(*) FROM test3;");
	is($result, qq(3),
	   "wal_level = $wal_level, optimized truncation with copied table");

	# Test truncation with inserted tuples using both INSERT and COPY. Tuples
	# inserted after the truncation should be seen.
	$node->safe_psql('postgres', "
		BEGIN;
		CREATE TABLE test4 (id serial PRIMARY KEY, id2 int);
		INSERT INTO test4 (id, id2) VALUES (DEFAULT, generate_series(1,10000));
		TRUNCATE test4;
		INSERT INTO test4 (id, id2) VALUES (DEFAULT, 10000);
		COPY test4 FROM '$copy_file' DELIMITER ',';
		INSERT INTO test4 (id, id2) VALUES (DEFAULT, 10000);
		COMMIT;");

	$node->stop('immediate');
	$node->start;
	$result = $node->safe_psql('postgres', "SELECT count(*) FROM test4;");
	is($result, qq(5),
	   "wal_level = $wal_level, optimized truncation with inserted/copied table");

	# Test consistency of COPY with INSERT for table created in the same
	# transaction.
	$node->safe_psql('postgres', "
		BEGIN;
		CREATE TABLE test5 (id serial PRIMARY KEY, id2 int);
		INSERT INTO test5 VALUES (DEFAULT, 1);
		COPY test5 FROM '$copy_file' DELIMITER ',';
		COMMIT;");
	$node->stop('immediate');
	$node->start;
	$result = $node->safe_psql('postgres', "SELECT count(*) FROM test5;");
	is($result, qq(4),
	   "wal_level = $wal_level, replay of optimized copy with inserted table");

	# Test consistency of COPY that inserts more to the same table using
	# triggers.  If the INSERTS from the trigger go to the same block data
	# is copied to, and the INSERTs are WAL-logged, WAL replay will fail when
	# it tries to replay the WAL record but the "before" image doesn't match,
	# because not all changes were WAL-logged.
	$node->safe_psql('postgres', "
		BEGIN;
		CREATE TABLE test6 (id serial PRIMARY KEY, id2 text);
		CREATE FUNCTION test6_before_row_trig() RETURNS trigger
		  LANGUAGE plpgsql as \$\$
		  BEGIN
		    IF new.id2 NOT LIKE 'triggered%' THEN
		      INSERT INTO test6 VALUES (DEFAULT, 'triggered row before' || NEW.id2);
		    END IF;
		    RETURN NEW;
		  END; \$\$;
		CREATE FUNCTION test6_after_row_trig() RETURNS trigger
		  LANGUAGE plpgsql as \$\$
		  BEGIN
		    IF new.id2 NOT LIKE 'triggered%' THEN
		      INSERT INTO test6 VALUES (DEFAULT, 'triggered row after' || OLD.id2);
		    END IF;
		    RETURN NEW;
		  END; \$\$;
		CREATE TRIGGER test6_before_row_insert
		  BEFORE INSERT ON test6
		  FOR EACH ROW EXECUTE PROCEDURE test6_before_row_trig();
		CREATE TRIGGER test6_after_row_insert
		  AFTER INSERT ON test6
		  FOR EACH ROW EXECUTE PROCEDURE test6_after_row_trig();
		COPY test6 FROM '$copy_file' DELIMITER ',';
		COMMIT;");
	$node->stop('immediate');
	$node->start;
	$result = $node->safe_psql('postgres', "SELECT count(*) FROM test6;");
	is($result, qq(9),
	   "wal_level = $wal_level, replay of optimized copy with before trigger");

	# Test consistency of INSERT, COPY and TRUNCATE in same transaction block
	# with TRUNCATE triggers.
	$node->safe_psql('postgres', "
		BEGIN;
		CREATE TABLE test7 (id serial PRIMARY KEY, id2 text);
		CREATE FUNCTION test7_before_stat_trig() RETURNS trigger
		  LANGUAGE plpgsql as \$\$
		  BEGIN
		    INSERT INTO test7 VALUES (DEFAULT, 'triggered stat before');
		    RETURN NULL;
		  END; \$\$;
		CREATE FUNCTION test7_after_stat_trig() RETURNS trigger
		  LANGUAGE plpgsql as \$\$
		  BEGIN
		    INSERT INTO test7 VALUES (DEFAULT, 'triggered stat before');
		    RETURN NULL;
		  END; \$\$;
		CREATE TRIGGER test7_before_stat_truncate
		  BEFORE TRUNCATE ON test7
		  FOR EACH STATEMENT EXECUTE PROCEDURE test7_before_stat_trig();
		CREATE TRIGGER test7_after_stat_truncate
		  AFTER TRUNCATE ON test7
		  FOR EACH STATEMENT EXECUTE PROCEDURE test7_after_stat_trig();
		INSERT INTO test7 VALUES (DEFAULT, 1);
		TRUNCATE test7;
		COPY test7 FROM '$copy_file' DELIMITER ',';
		COMMIT;");
	$node->stop('immediate');
	$node->start;
	$result = $node->safe_psql('postgres', "SELECT count(*) FROM test7;");
	is($result, qq(4),
	   "wal_level = $wal_level, replay of optimized copy with before trigger");

	$node->teardown_node;
	$node->clean_node;
	return;
}

# Run same test suite for multiple wal_level values.
run_wal_optimize("minimal");
run_wal_optimize("replica");
