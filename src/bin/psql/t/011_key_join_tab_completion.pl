# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Data::Dumper;

# Do nothing unless Makefile has told us that the build is --with-readline.
if (!defined($ENV{with_readline}) || $ENV{with_readline} ne 'yes')
{
	plan skip_all => 'readline is not supported by this build';
}

# Also, skip if user has set environment variable to command that.
# This is mainly intended to allow working around some of the more broken
# versions of libedit --- some users might find them acceptable even if
# they won't pass these tests.
if (defined($ENV{SKIP_READLINE_TESTS}))
{
	plan skip_all => 'SKIP_READLINE_TESTS is set';
}

# If we don't have IO::Pty, forget it, because IPC::Run depends on that
# to support pty connections.
eval { require IO::Pty; };
if ($@)
{
	plan skip_all => 'IO::Pty is needed to run this test';
}

# Start a new server.
my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;

# Set up relations with enough foreign keys to exercise FOR KEY completion.
$node->safe_psql('postgres',
		"CREATE TABLE key_join_parent (id int PRIMARY KEY);\n"
	  . "CREATE TABLE key_join_child"
	  . " (parent_id int REFERENCES key_join_parent(id));\n"
	  . "CREATE TABLE key_join_customers (id int PRIMARY KEY);\n"
	  . "CREATE TABLE key_join_orders (id int PRIMARY KEY,"
	  . " customer_id int REFERENCES key_join_customers(id));\n"
	  . "CREATE TABLE key_join_order_items (id int PRIMARY KEY,"
	  . " order_id int REFERENCES key_join_orders(id));\n"
	  . "CREATE TABLE key_join_departments (dept_id int PRIMARY KEY);\n"
	  . "CREATE TABLE key_join_employees (emp_id int PRIMARY KEY,"
	  . " dept_id int REFERENCES key_join_departments(dept_id),"
	  . " home_dept int REFERENCES key_join_departments(dept_id));\n"
	  . "CREATE TABLE key_join_named_departments (dept_id int"
	  . " PRIMARY KEY, name text UNIQUE, active boolean DEFAULT true);\n"
	  . "CREATE TABLE key_join_named_employees (emp_id int PRIMARY KEY,"
	  . " dept_id int REFERENCES key_join_named_departments(dept_id),"
	  . " home_dept int REFERENCES key_join_named_departments(dept_id),"
	  . " dept_name text REFERENCES key_join_named_departments(name));\n"
	  . "CREATE TABLE key_join_no_fk_left (id int PRIMARY KEY);\n"
	  . "CREATE TABLE key_join_no_fk_right (id int PRIMARY KEY);\n"
	  . "CREATE TABLE key_join_staff (staff_id int PRIMARY KEY,"
	  . " boss_id int REFERENCES key_join_staff(staff_id));\n"
	  . "CREATE TABLE key_join_users (user_id int, org_id int,"
	  . " PRIMARY KEY (org_id, user_id));\n"
	  . "CREATE TABLE key_join_posts (author_id int, org_id int,"
	  . " FOREIGN KEY (org_id, author_id)"
	  . " REFERENCES key_join_users (org_id, user_id));\n"
	  . "CREATE SCHEMA key_join_schema;\n"
	  . "CREATE TABLE key_join_schema.\"Key Join Referenced\""
	  . " (\"Ref ID\" int PRIMARY KEY);\n"
	  . "CREATE TABLE key_join_schema.\"Key Join Referencing\""
	  . " (\"Ref ID\" int REFERENCES"
	  . " key_join_schema.\"Key Join Referenced\"(\"Ref ID\"));\n"
	  . "CREATE TABLE key_join_q_ref (\"r)id\" int PRIMARY KEY);\n"
	  . "CREATE TABLE key_join_q_fk (\"a)x\" int"
	  . " REFERENCES key_join_q_ref(\"r)id\"),"
	  . " \"b)x\" int REFERENCES key_join_q_ref(\"r)id\"));\n");

# Arrange to capture, not discard, the interactive session's history output.
# Put it in the test log directory, so that buildfarm runs capture the result
# for possible debugging purposes.
my $historyfile = "${PostgreSQL::Test::Utils::log_path}/011_psql_history.txt";

# Fire up an interactive psql session and configure it such that each query
# restarts the timer.
my $h = $node->interactive_psql('postgres', history_file => $historyfile);
$h->set_query_timer_restart();

sub check_completion
{
	my ($send, $pattern, $annotation) = @_;

	# report test failures from caller location
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	# send the data to be sent and wait for its result
	my $out = $h->query_until($pattern, $send);
	my $okay = ($out =~ $pattern && !$h->{timeout}->is_expired);
	ok($okay, $annotation);
	# for debugging, log actual output if it didn't match
	local $Data::Dumper::Terse = 1;
	local $Data::Dumper::Useqq = 1;
	diag 'Actual output was ' . Dumper($out) . "Did not match \"$pattern\"\n"
	  if !$okay;
	return;
}

# Clear query buffer to start over
# (won't work if we are inside a string literal!)
sub clear_query
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	check_completion("\\r\n", qr/Query buffer reset.*postgres=# /s,
		"\\r works");
	return;
}

# Like check_completion, but expect the output NOT to match the pattern;
# the line is then abandoned with control-U
sub check_no_completion_match
{
	my ($send, $pattern, $annotation) = @_;

	local $Test::Builder::Level = $Test::Builder::Level + 1;

	my $out = $h->query_until(qr/postgres=# /s, $send . "\025\n");
	my $okay = ($out !~ $pattern && !$h->{timeout}->is_expired);
	ok($okay, $annotation);
	local $Data::Dumper::Terse = 1;
	local $Data::Dumper::Useqq = 1;
	diag 'Actual output was '
	  . Dumper($out)
	  . "Unexpectedly matched \"$pattern\"\n"
	  if !$okay;
	return;
}

# check tab completion for FOR KEY joins

# When more than one foreign key matches the typed prefix, completion
# advances one phrase at a time: a phrase shared by every matching clause
# completes by itself, and the menu then shows only the next, diverging
# phrase of each clause.
# SELECT * FROM key_join_employees LEFT JOIN key_join_departments f\t
# SELECT * FROM key_join_employees LEFT JOIN key_join_departments for key (
check_completion(
	"SELECT * FROM key_join_employees LEFT JOIN key_join_departments f\t",
	qr/f\x07?or key \( (?!dept_id)/i,
	"complete shared FOR KEY ( phrase of ambiguous join");

# \t -> dept_id ) ; the referenced columns are the same in both clauses
# (libedit lists even a sole match completed from an empty word, while
# readline inserts it silently; match just the inserted text)
check_completion("\t", qr/dept_id \)/,
	"complete shared referenced-columns phrase of ambiguous join");

# \t -> <- key_join_employees ( ; direction and relation are shared too
check_completion("\t", qr/<- key_join_employees \(/,
	"complete shared direction phrase of ambiguous join");

# \t\t -> only the diverging referencing columns appear in the menu
check_completion("\t\t", qr/dept_id \) +home_dept \)/,
	"offer only the diverging phrase of ambiguous join");

clear_query();

# SELECT * FROM key_join_employees LEFT JOIN key_join_departments
#   for key ( dept_id ) <- key_join_employees ( \t\t
# dept_id )  home_dept )
check_completion(
	"SELECT * FROM key_join_employees LEFT JOIN key_join_departments "
	  . "for key ( dept_id ) <- key_join_employees ( \t\t",
	qr/dept_id \) +home_dept \)/,
	"offer suffix alternatives from canonical typed prefix");

clear_query();

# SELECT * FROM key_join_orders JOIN key_join_customers f\t
# SELECT * FROM key_join_orders JOIN key_join_customers
#   for key ( id ) <- key_join_orders ( customer_id )
check_completion(
	"SELECT * FROM key_join_orders JOIN key_join_customers f\t",
	qr/f\x07?or key \( id \) <- key_join_orders \( customer_id \)/i,
	"complete unique FOR KEY join from join-condition prefix");

clear_query();

# SELECT * FROM key_join_orders JOIN key_join_customers FOR K\t
# SELECT * FROM key_join_orders JOIN key_join_customers
#   FOR KEY ( id ) <- key_join_orders ( customer_id )
check_completion(
	"SELECT * FROM key_join_orders JOIN key_join_customers FOR K\t",
	qr/K\x07?EY \( id \) <- key_join_orders \( customer_id \)/,
	"complete unique FOR KEY join after FOR K");

clear_query();

# SELECT * FROM key_join_orders JOIN key_join_customers FOR \t
# SELECT * FROM key_join_orders JOIN key_join_customers
#   FOR KEY ( id ) <- key_join_orders ( customer_id )
check_completion(
	"SELECT * FROM key_join_orders JOIN key_join_customers FOR \t",
	qr/KEY \( id \) <- key_join_orders \( customer_id \)/,
	"complete unique FOR KEY join after FOR");

clear_query();

# SELECT * FROM key_join_orders JOIN key_join_customers for \t
# SELECT * FROM key_join_orders JOIN key_join_customers
#   for key ( id ) <- key_join_orders ( customer_id )
check_completion(
	"SELECT * FROM key_join_orders JOIN key_join_customers for \t",
	qr/key \( id \) <- key_join_orders \( customer_id \)/,
	"complete unique FOR KEY join preserving lowercase FOR");

clear_query();

# SELECT * FROM key_join_orders JOIN key_join_customers for k\t
# SELECT * FROM key_join_orders JOIN key_join_customers
#   for key ( id ) <- key_join_orders ( customer_id )
check_completion(
	"SELECT * FROM key_join_orders JOIN key_join_customers for k\t",
	qr/k\x07?ey \( id \) <- key_join_orders \( customer_id \)/,
	"complete unique FOR KEY join after lowercase for k");

clear_query();

# SELECT * FROM key_join_orders JOIN key_join_customers \t\t
# FOR KEY (  ON  USING (
check_completion(
	"SELECT * FROM key_join_orders JOIN key_join_customers \t\t",
	qr/(?=.*FOR KEY \()(?=.*ON)(?=.*USING \()/s,
	"offer ordinary join conditions including FOR KEY");

clear_query();

# SELECT * FROM key_join_no_fk_left JOIN key_join_no_fk_right FOR \t
# SELECT * FROM key_join_no_fk_left JOIN key_join_no_fk_right FOR
check_no_completion_match(
	"SELECT * FROM key_join_no_fk_left JOIN key_join_no_fk_right FOR \t",
	qr/KEY \(/,
	"do not infer FOR KEY join after FOR without foreign key");

clear_query();

# SELECT * FROM key_join_no_fk_left JOIN key_join_no_fk_right f\t
# SELECT * FROM key_join_no_fk_left JOIN key_join_no_fk_right for key (
check_completion(
	"SELECT * FROM key_join_no_fk_left JOIN key_join_no_fk_right f\t",
	qr/f\x07?or key \((?!\s*id)/i,
	"complete ordinary FOR KEY prefix without foreign key inference");

clear_query();

# SELECT * FROM key_join_order_items i JOIN key_join_orders o
#   FOR KEY ( id ) <- i ( order_id ) JOIN key_join_customers FOR \t
# SELECT * FROM key_join_order_items i JOIN key_join_orders o
#   FOR KEY ( id ) <- i ( order_id ) JOIN key_join_customers
#   FOR KEY ( id ) <- o ( customer_id )
check_completion(
	"SELECT * FROM key_join_order_items i JOIN key_join_orders o "
	  . "FOR KEY ( id ) <- i ( order_id ) "
	  . "JOIN key_join_customers FOR \t",
	qr/KEY \( id \) <- o \( customer_id \)/,
	"complete FOR KEY join using visible alias");

clear_query();

# SELECT * FROM key_join_orders AS o JOIN key_join_customers FOR \t
# SELECT * FROM key_join_orders AS o JOIN key_join_customers
#   FOR KEY ( id ) <- o ( customer_id )
check_completion(
	"SELECT * FROM key_join_orders AS o JOIN key_join_customers FOR \t",
	qr/KEY \( id \) <- o \( customer_id \)/,
	"complete FOR KEY join preserving AS alias");

clear_query();

# SELECT * FROM key_join_orders key JOIN key_join_customers FOR \t
# SELECT * FROM key_join_orders key JOIN key_join_customers
#   FOR KEY ( id ) <- key ( customer_id )
check_completion(
	"SELECT * FROM key_join_orders key JOIN key_join_customers FOR \t",
	qr/KEY \( id \) <- key \( customer_id \)/,
	"complete FOR KEY join preserving alias named key");

clear_query();

# SELECT * FROM key_join_parent JOIN key_join_child FOR \t
# SELECT * FROM key_join_parent JOIN key_join_child
#   FOR KEY ( parent_id ) -> key_join_parent ( id )
check_completion(
	"SELECT * FROM key_join_parent JOIN key_join_child FOR \t",
	qr/KEY \( parent_id \) -> key_join_parent \( id \)/,
	"complete FOR KEY join when right side has foreign key");

clear_query();

# SELECT * FROM key_join_named_employees JOIN key_join_named_departments
#   FOR KEY ( name ) <- key_join_named_employees ( \t
# SELECT * FROM key_join_named_employees JOIN key_join_named_departments
#   FOR KEY ( name ) <- key_join_named_employees ( dept_name )
check_completion(
	"SELECT * FROM key_join_named_employees "
	  . "JOIN key_join_named_departments FOR KEY ( name ) "
	  . "<- key_join_named_employees ( \t",
	qr/dept_name \)/,
	"complete unique suffix from canonical typed prefix");

clear_query();

# The key columns of the multicolumn foreign key are deliberately not in
# attnum order, so this also verifies that columns appear in key order.
# SELECT * FROM key_join_posts JOIN key_join_users FOR KEY ( org_id, u\t
# SELECT * FROM key_join_posts JOIN key_join_users
#   FOR KEY ( org_id, user_id ) <- key_join_posts ( org_id, author_id )
check_completion(
	"SELECT * FROM key_join_posts JOIN key_join_users "
	  . "FOR KEY ( org_id, u\t",
	qr/u\x07?ser_id \) <- key_join_posts \( org_id, author_id \)/,
	"complete multicolumn FOR KEY join from canonical typed prefix");

clear_query();

# A self-referencing foreign key in a self-join can be used in either
# direction.  The KEY ( phrase is shared by both directions; the menu
# then offers only their key columns.
# SELECT * FROM key_join_staff s1 JOIN key_join_staff s2 FOR \t
# SELECT * FROM key_join_staff s1 JOIN key_join_staff s2 FOR KEY (
check_completion(
	"SELECT * FROM key_join_staff s1 JOIN key_join_staff s2 FOR \t",
	qr/FOR KEY \(/,
	"complete shared KEY ( phrase of self-referencing foreign key");

# \t\t -> menu with the key columns of both directions
check_completion("\t\t", qr/boss_id \) +staff_id \)/,
	"offer both directions for self-referencing foreign key");

# s\t -> the single matching direction completes in full
check_completion("s\t", qr/s\x07?taff_id \) <- s1 \( boss_id \)/,
	"complete one direction after disambiguating menu");

clear_query();

# SELECT * FROM key_join_staff s1 JOIN key_join_staff s2 FOR KEY ( s\t
# SELECT * FROM key_join_staff s1 JOIN key_join_staff s2
#   FOR KEY ( staff_id ) <- s1 ( boss_id )
check_completion(
	"SELECT * FROM key_join_staff s1 JOIN key_join_staff s2 "
	  . "FOR KEY ( s\t",
	qr/s\x07?taff_id \) <- s1 \( boss_id \)/,
	"complete one direction of self-referencing FOR KEY join");

clear_query();

# SELECT * FROM key_join_schema."Key Join Referencing"
#   JOIN key_join_schema."Key Join Referenced" FOR \t
# SELECT * FROM key_join_schema."Key Join Referencing"
#   JOIN key_join_schema."Key Join Referenced"
#   FOR KEY ( "Ref ID" ) <- "Key Join Referencing" ( "Ref ID" )
# This completed line is long enough to exceed the test terminal's width and
# wrap, scrolling the leading "KEY (" phrase out of the captured output, so
# match only from the arrow onward (which stays on screen), as other long
# cases do.
check_completion(
	"SELECT * FROM key_join_schema.\"Key Join Referencing\" "
	  . "JOIN key_join_schema.\"Key Join Referenced\" FOR \t",
	qr/<- "Key Join Referencing" \( "Ref ID" \)/,
	"complete FOR KEY join with quoted schema-qualified identifiers");

clear_query();

# Phrase truncation must not split a quoted identifier containing a
# parenthesis: the shared "r)id" ) phrase completes whole, and the menu
# offers the quoted referencing columns.
# SELECT * FROM key_join_q_fk f JOIN key_join_q_ref for key ( \t
# SELECT * FROM key_join_q_fk f JOIN key_join_q_ref for key ( "r)id" )
check_completion(
	"SELECT * FROM key_join_q_fk f JOIN key_join_q_ref for key ( \t",
	qr/for key \( "r\)id" \)/,
	"complete shared phrase without splitting quoted parenthesis");

# \t -> <- f ( ; the direction phrase is shared as well
check_completion("\t", qr/<- f \(/,
	"complete shared direction phrase with quoted key columns");

# \t -> " ; the quote is the longest common prefix of the alternatives
check_completion("\t", qr/"/,
	"insert common quote of diverging quoted columns");

# \t\t -> menu with the two quoted referencing columns
check_completion("\t\t", qr/"a\)x" \) +"b\)x" \)/,
	"offer quoted referencing columns in menu");

# a\t -> the rest of the single matching clause, balancing the quotes
check_completion("a\t", qr/a\x07?\)x" \) /,
	"complete quoted column after disambiguation");

clear_query();

# Send psql an explicit \q to shut it down, else pty won't close properly.
$h->quit or die "psql returned $?";

$node->stop;
done_testing();
