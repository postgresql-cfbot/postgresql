
# Copyright (c) 2021, PostgreSQL Global Development Group

# Test some logical replication DDL behavior
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 7;

my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->start;

my $ddl = "CREATE TABLE test1 (a int, b text);";
$node_publisher->safe_psql('postgres', $ddl);
$node_subscriber->safe_psql('postgres', $ddl);

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';

$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION mypub FOR ALL TABLES;");
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION mysub CONNECTION '$publisher_connstr' PUBLICATION mypub;"
);

$node_publisher->wait_for_catchup('mysub');

$node_subscriber->safe_psql(
	'postgres', q{
BEGIN;
ALTER SUBSCRIPTION mysub DISABLE;
ALTER SUBSCRIPTION mysub SET (slot_name = NONE);
DROP SUBSCRIPTION mysub;
COMMIT;
});

pass "subscription disable and drop in same transaction did not hang";

# One of the specified publications exists.
my ($ret, $stdout, $stderr) = $node_subscriber->psql('postgres',
	"CREATE SUBSCRIPTION mysub1 CONNECTION '$publisher_connstr' PUBLICATION mypub, non_existent_pub WITH (VALIDATE_PUBLICATION = TRUE)"
);
ok( $stderr =~
	  m/ERROR:  publication "non_existent_pub" does not exist in the publisher/,
	"Create subscription fails with single non-existent publication");

# Specifying multiple non-existent publications.
($ret, $stdout, $stderr) = $node_subscriber->psql('postgres',
	"CREATE SUBSCRIPTION mysub1 CONNECTION '$publisher_connstr' PUBLICATION non_existent_pub, non_existent_pub1 WITH (VALIDATE_PUBLICATION = TRUE)"
);
ok( $stderr =~
	  m/ERROR:  publications "non_existent_pub", "non_existent_pub1" do not exist in the publisher/,
	"Create subscription fails with multiple non-existent publications");

# Specifying mutually exclusive options.
($ret, $stdout, $stderr) = $node_subscriber->psql('postgres',
	"CREATE SUBSCRIPTION mysub1 CONNECTION '$publisher_connstr' PUBLICATION non_existent_pub, non_existent_pub1 WITH (CONNECT = FALSE, VALIDATE_PUBLICATION = TRUE)"
);
ok( $stderr =~
	  m/ERROR:  connect = false and validate_publication = true are mutually exclusive options/,
	"Create subscription fails with mutually exclusive options");

# Specifying non-existent publication along with add publication.
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION mysub1 CONNECTION '$publisher_connstr' PUBLICATION mypub;"
);
($ret, $stdout, $stderr) = ($ret, $stdout, $stderr) = $node_subscriber->psql(
	'postgres',
	"ALTER SUBSCRIPTION mysub1 ADD PUBLICATION non_existent_pub WITH (REFRESH = FALSE, VALIDATE_PUBLICATION = TRUE)"
);
ok( $stderr =~
	  m/ERROR:  publication "non_existent_pub" does not exist in the publisher/,
	"Alter subscription add publication fails with non-existent publication");

# Specifying non-existent publication along with set publication.
($ret, $stdout, $stderr) = $node_subscriber->psql('postgres',
	"ALTER SUBSCRIPTION mysub1 SET PUBLICATION non_existent_pub WITH (VALIDATE_PUBLICATION = TRUE)"
);
ok( $stderr =~
	  m/ERROR:  publication "non_existent_pub" does not exist in the publisher/,
	"Alter subscription set publication fails with non-existent publication");

# Specifying non-existent database.
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION mysub1 CONNECTION 'dbname=regress_doesnotexist2'");
($ret, $stdout, $stderr) = $node_subscriber->psql('postgres',
	"ALTER SUBSCRIPTION mysub1 SET PUBLICATION non_existent_pub WITH (REFRESH = FALSE, VALIDATE_PUBLICATION = TRUE)"
);
ok( $stderr =~ m/ERROR:  could not connect to the publisher/,
	"Alter subscription set publication fails with connection to a non-existent database"
);

$node_subscriber->stop;
$node_publisher->stop;
