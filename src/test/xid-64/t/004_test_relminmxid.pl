# Check integrity after dump/restore with different xids
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use bigint;

my ($node, $rmm, $vacout);
$node = PostgreSQL::Test::Cluster->new('master');
$node->init(extra => [ "--xid=3", "--multixact-id=1", "--multixact-offset=0" ]);
$node->append_conf('postgresql.conf', 'max_prepared_transactions = 2');
$node->start;

sub relminmxid
{
    my $rmm = $node->safe_psql("postgres", qq(
        SELECT relminmxid
        FROM pg_class
        WHERE relname = 'foo';));
    return $rmm + 0;
}

sub vacuum
{
    my ($rc, $stdout, $stderr) = $node->psql("postgres", "VACUUM foo;");
    return $stdout.$stderr;
}

sub gen_multixact
{
    $node->safe_psql("postgres", qq(
    BEGIN;
        SELECT * FROM foo FOR KEY SHARE;
    PREPARE TRANSACTION 'fooshare';
    ));

    my $xmax = $node->safe_psql("postgres", qq(
        SELECT xmax FROM foo;
    ));
    isnt($xmax + 0, 0, "xmax not empty");

    $node->safe_psql("postgres", qq(
    BEGIN;
        SELECT * FROM foo FOR KEY SHARE;
    COMMIT;
    COMMIT PREPARED 'fooshare';
    ));

    my $mxact = $node->safe_psql("postgres", qq(
        SELECT xmax FROM foo;
    ));
    isnt($mxact + 0, 0, "mxact not empty");
    cmp_ok($xmax, '>', $mxact, "xmax is greater than mxact");
}

# Initialize master node with the random xid-related parameters
$node->safe_psql("postgres", "CREATE TABLE foo (a int); INSERT INTO foo VALUES (1);");

is(relminmxid(), 1, "relminmxid is default");

vacuum();
is(relminmxid(), 1, "relminmxid is still default");

gen_multixact();
is(relminmxid(), 1, "relminmxid is still still default");

unlike(vacuum(), qr/multixact.*before relminmxid/, "no relminmxid error");

# No intentionally break relminmxid
$node->safe_psql("postgres", qq(
    UPDATE pg_class SET relminmxid = ((1::int8<<62) + 1)::text::xid
    WHERE relname = 'foo'
));
cmp_ok(relminmxid(), '>', 2**62, "relminmxid broken (intentionally)");

gen_multixact();
like(vacuum(), qr/multixact.*before relminmxid/, "got relminmxid error");
cmp_ok(relminmxid(), '>', 2**62, "relminmxid broken (still)");

# Fix relminmxid by setting to default
$node->safe_psql("postgres", qq(
    UPDATE pg_class SET relminmxid = '1'
    WHERE relname = 'foo'
));
is(relminmxid(), 1, "relminmxid is default again");

unlike(vacuum(), qr/multixact.*before relminmxid/, "no relminmxid error again");

done_testing();
