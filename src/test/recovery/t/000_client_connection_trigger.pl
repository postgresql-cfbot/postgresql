use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;
if (!$use_unix_sockets)
{
	plan skip_all =>
	  "authentication tests cannot run without Unix-domain sockets";
}
else
{
	plan tests => 5;
}

# Initialize master node
my $node = get_new_node('master');
$node->init;
$node->start;
$node->safe_psql('postgres', q{
CREATE ROLE regress_user LOGIN PASSWORD 'pass';
CREATE ROLE regress_hacker LOGIN PASSWORD 'pass';

CREATE TABLE connects(id serial, who text);

CREATE FUNCTION on_login_proc() RETURNS EVENT_TRIGGER AS $$
BEGIN
  IF NOT pg_is_in_recovery() THEN
    INSERT INTO connects (who) VALUES (session_user);
  END IF;
  IF session_user = 'regress_hacker' THEN
    RAISE EXCEPTION 'You are not welcome!';
  END IF;
  RAISE NOTICE 'You are welcome!';
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE EVENT TRIGGER on_login_trigger ON client_connection EXECUTE FUNCTION on_login_proc();
ALTER EVENT TRIGGER on_login_trigger ENABLE ALWAYS;
}
);
my $res;

$res = $node->safe_psql('postgres', "SELECT 1");

$res = $node->safe_psql('postgres', "SELECT 1",
  extra_params => [ '-U', 'regress_user', '-w' ]);

my ($ret, $stdout, $stderr) = $node->psql('postgres', "SELECT 1",
  extra_params => [ '-U', 'regress_hacker', '-w' ]);
ok( $ret != 0 && $stderr =~ /You are not welcome!/ );

$res = $node->safe_psql('postgres', "SELECT COUNT(1) FROM connects WHERE who = 'regress_user'");
ok($res == 1);

my $tempdir = TestLib::tempdir;
command_ok(
  [ "pg_dumpall", '-p', $node->port, '-c', "--file=$tempdir/regression_dump.sql", ],
  "dumpall");
# my $dump_contents = slurp_file("$tempdir/regression_dump.sql");
# print($dump_contents);

my $node1 = get_new_node('secondary');
$node1->init;
$node1->start;
command_ok(["psql", '-p', $node1->port, '-b', '-f', "$tempdir/regression_dump.sql" ] );
$res = $node1->safe_psql('postgres', "SELECT 1", extra_params => [ '-U', 'regress_user', '-w' ]);
$res = $node1->safe_psql('postgres', "SELECT COUNT(1) FROM connects WHERE who = 'regress_user'");
ok($res == 2);
