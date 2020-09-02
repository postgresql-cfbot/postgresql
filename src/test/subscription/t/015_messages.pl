# Tests that logical decoding messages are emitted and that
# they do not break subscribers
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 6;

my $node_publisher = get_new_node('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

my $node_subscriber = get_new_node('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->start;

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
#
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tab (a int PRIMARY KEY)");
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab (a int PRIMARY KEY)");
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION pub FOR TABLE tab");
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub CONNECTION '$publisher_connstr' PUBLICATION pub"
);

# ensure a transactional logical decoding message shows up on the slot
$node_subscriber->safe_psql('postgres', "ALTER SUBSCRIPTION sub DISABLE");

$node_publisher->safe_psql('postgres',
	"select pg_logical_emit_message(true, 'a prefix', 'a transactional message')");

my $slot_codes_with_message = $node_publisher->safe_psql(
	'postgres', qq(
		select get_byte(data, 0)
		from pg_logical_slot_peek_binary_changes('sub', NULL, NULL,
			'proto_version', '1', 'publication_names', 'pub', 'messages', 'true')
));

# 66 77 67 == B M C == BEGIN MESSAGE COMMIT
is($slot_codes_with_message, "66\n77\n67",
	'messages on slot are B M C with message option');

my $transactional_message_flags = $node_publisher->safe_psql(
	'postgres', qq(
		select get_byte(data, 1)
		from pg_logical_slot_peek_binary_changes('sub', NULL, NULL,
			'proto_version', '1', 'publication_names', 'pub', 'messages', 'true')
    offset 1 limit 1
));

is($transactional_message_flags, "1",
  "transactional message flags are set to 1");

my $slot_codes_without_message = $node_publisher->safe_psql(
	'postgres', qq(
		select get_byte(data, 0)
		from pg_logical_slot_peek_binary_changes('sub', NULL, NULL,
			'proto_version', '1', 'publication_names', 'pub')
));

# 66 67 == B C == BEGIN COMMIT
is($slot_codes_without_message, "66\n67",
	'messages on slot are B C without message option');

$node_subscriber->safe_psql('postgres', "ALTER SUBSCRIPTION sub ENABLE");
$node_publisher->wait_for_catchup('sub');

# ensure a non-transactional logical decoding message shows up on the slot
$node_subscriber->safe_psql('postgres', "ALTER SUBSCRIPTION sub DISABLE");

$node_publisher->safe_psql('postgres', "INSERT INTO tab VALUES (3)");

my $message_lsn = $node_publisher->safe_psql('postgres',
	"select pg_logical_emit_message(false, 'prefix', 'nontransactional')");

$node_publisher->safe_psql('postgres', "INSERT INTO tab VALUES (4)");

my $slot_message_code = $node_publisher->safe_psql(
	'postgres', qq(
  select get_byte(data, 0)
  from pg_logical_slot_peek_binary_changes('sub', NULL, NULL,
    'proto_version', '1', 'publication_names', 'pub', 'messages', 'true')
  where lsn = '$message_lsn' and xid = 0
));

is($slot_message_code, "77", "non-transactional message on slot is M");

my $nontransactional_message_flags = $node_publisher->safe_psql(
	'postgres', qq(
		select get_byte(data, 1)
		from pg_logical_slot_peek_binary_changes('sub', NULL, NULL,
			'proto_version', '1', 'publication_names', 'pub', 'messages', 'true')
    offset 1 limit 1
));

is($nontransactional_message_flags, "0",
  "non-transactional message flags are set to 0");


$node_subscriber->safe_psql('postgres', "ALTER SUBSCRIPTION sub ENABLE");
$node_publisher->wait_for_catchup('sub');

my $result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM tab");
is($result, qq(2), 'rows move');

$node_subscriber->stop('fast');
$node_publisher->stop('fast');
