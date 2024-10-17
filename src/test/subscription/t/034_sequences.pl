
# Copyright (c) 2024, PostgreSQL Global Development Group

# This tests that sequences are synced correctly to the subscriber
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Initialize publisher node
my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');

# Avoid checkpoint during the test, otherwise, extra values will be fetched for
# the sequences which will cause the test to fail randomly.
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf('postgresql.conf', 'checkpoint_timeout = 1h');
$node_publisher->start;

# Initialize subscriber node
my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->start;

# Setup structure on the publisher
my $ddl = qq(
	CREATE TABLE regress_seq_test (v BIGINT);
	CREATE SEQUENCE regress_s1;
);
$node_publisher->safe_psql('postgres', $ddl);

# Setup the same structure on the subscriber, plus some extra sequences that
# we'll create on the publisher later
$ddl = qq(
	CREATE TABLE regress_seq_test (v BIGINT);
	CREATE SEQUENCE regress_s1;
	CREATE SEQUENCE regress_s2;
	CREATE SEQUENCE regress_s3;
);
$node_subscriber->safe_psql('postgres', $ddl);

# Insert initial test data
$node_publisher->safe_psql(
	'postgres', qq(
	-- generate a number of values using the sequence
	INSERT INTO regress_seq_test SELECT nextval('regress_s1') FROM generate_series(1,100);
));

# Setup logical replication pub/sub
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION regress_seq_pub FOR ALL SEQUENCES");
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION regress_seq_sub CONNECTION '$publisher_connstr' PUBLICATION regress_seq_pub"
);

# Wait for initial sync to finish
my $synced_query =
  "SELECT count(1) = 0 FROM pg_subscription_rel WHERE srsubstate NOT IN ('r');";
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

# Check the initial data on subscriber
my $result = $node_subscriber->safe_psql(
	'postgres', qq(
	SELECT last_value, log_cnt, is_called FROM regress_s1;
));
is($result, '100|32|t', 'initial test data replicated');

##########
## ALTER SUBSCRIPTION ... REFRESH PUBLICATION should cause sync of new
# sequences of the publisher, but changes to existing sequences should
# not be synced.
##########

# Create a new sequence 'regress_s2', and update existing sequence 'regress_s1'
$node_publisher->safe_psql(
	'postgres', qq(
	CREATE SEQUENCE regress_s2;
	INSERT INTO regress_seq_test SELECT nextval('regress_s2') FROM generate_series(1,100);

    -- Existing sequence
	INSERT INTO regress_seq_test SELECT nextval('regress_s1') FROM generate_series(1,100);
));

# Do ALTER SUBSCRIPTION ... REFRESH PUBLICATION
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	ALTER SUBSCRIPTION regress_seq_sub REFRESH PUBLICATION
));
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

# Check - existing sequence is not synced
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	SELECT last_value, log_cnt, is_called FROM regress_s1;
));
is($result, '100|32|t',
	'REFRESH PUBLICATION does not sync existing sequence');

# Check - newly published sequence is synced
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	SELECT last_value, log_cnt, is_called FROM regress_s2;
));
is($result, '100|32|t',
	'REFRESH PUBLICATION will sync newly published sequence');

##########
## ALTER SUBSCRIPTION ... REFRESH PUBLICATION SEQUENCES should cause sync of
# new sequences of the publisher, and changes to existing sequences should
# also be synced.
##########

# Create a new sequence 'regress_s3', and update the existing sequence
# 'regress_s2'.
$node_publisher->safe_psql(
	'postgres', qq(
	CREATE SEQUENCE regress_s3;
	INSERT INTO regress_seq_test SELECT nextval('regress_s3') FROM generate_series(1,100);

	-- Existing sequence
	INSERT INTO regress_seq_test SELECT nextval('regress_s2') FROM generate_series(1,100);
));

# Do ALTER SUBSCRIPTION ... REFRESH PUBLICATION SEQUENCES
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	ALTER SUBSCRIPTION regress_seq_sub REFRESH PUBLICATION SEQUENCES
));
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

# Check - existing sequences are synced
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	SELECT last_value, log_cnt, is_called FROM regress_s1;
));
is($result, '200|31|t',
	'REFRESH PUBLICATION SEQUENCES will sync existing sequences');
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	SELECT last_value, log_cnt, is_called FROM regress_s2;
));
is($result, '200|31|t',
	'REFRESH PUBLICATION SEQUENCES will sync existing sequences');

# Check - newly published sequence is synced
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	SELECT last_value, log_cnt, is_called FROM regress_s3;
));
is($result, '100|32|t',
	'REFRESH PUBLICATION SEQUENCES will sync newly published sequence');

##########
# ALTER SUBSCRIPTION ... REFRESH PUBLICATION SEQUENCES should throw a warning
# for sequence definition not matching between the publisher and the subscriber.
##########

# Create a new sequence 'regress_s4' whose START value is not the same in the
# publisher and subscriber.
$node_publisher->safe_psql(
	'postgres', qq(
	CREATE SEQUENCE regress_s4 START 1 INCREMENT 2;
));

$node_subscriber->safe_psql(
	'postgres', qq(
	CREATE SEQUENCE regress_s4 START 10 INCREMENT 2;
));

my $log_offset = -s $node_subscriber->logfile;

# Do ALTER SUBSCRIPTION ... REFRESH PUBLICATION SEQUENCES
$node_subscriber->safe_psql(
	'postgres', "
    ALTER SUBSCRIPTION regress_seq_sub REFRESH PUBLICATION SEQUENCES"
);

# Confirm that the warning for parameters differing is logged.
$node_subscriber->wait_for_log(
	qr/WARNING: ( [A-Z0-9]+:)? parameters differ for the remote and local sequences \("public.regress_s4"\) for subscription "regress_seq_sub"/,
	$log_offset);
done_testing();
