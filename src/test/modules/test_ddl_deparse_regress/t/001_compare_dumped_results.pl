use strict;
use warnings;
use Env;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use File::Basename;

sub execute_test_case {
    my $test_name = $_[0];
    my $pub_node = $_[1];
    my $sub_node = $_[2];
    my $dbname = $_[3];
    my $outputdir = $PostgreSQL::Test::Utils::tmp_check;

    # set up deparse testing resources
    create_deparse_testing_resources_on_pub_node($pub_node, $dbname);

    my $test_file = "./sql/${test_name}.sql";
    my $content = do{local(@ARGV,$/)=$test_file;<>};

    $pub_node -> psql($dbname, $content);

    # retrieve SQL commands generated from deparsed DDLs on pub node
    my $ddl_sql = '';
    $pub_node -> psql($dbname,q(
        select ddl_deparse_expand_command(ddl) || ';' from deparsed_ddls ORDER BY id ASC),
        stdout => \$ddl_sql);

    print "\nstart printing re-formed sql\n";
    print $ddl_sql;
    print "\nend printing re-formed sql\n";
    # execute SQL commands on sub node
    $sub_node -> psql($dbname, $ddl_sql);

    # clean up deparse testing resources
    clean_deparse_testing_resources_on_pub_node($pub_node, $dbname);

    # dump from pub node and sub node
    mkdir ${outputdir}."/dumps", 0755;
    my $pub_dump = ${outputdir}."/dumps/${test_name}_pub.dump";
    my $sub_dump = ${outputdir}."/dumps/${test_name}_sub.dump";
    system("pg_dumpall "
        . "-s "
        . "-f "
        . $pub_dump . " "
        . "--no-sync "
        .  '-p '
        . $pub_node->port)  == 0 or die "Dump pub node failed in ${test_name}";
    system("pg_dumpall "
        . "-s "
        . "-f "
        . $sub_dump . " "
        . "--no-sync "
        .  '-p '
        . $sub_node->port)  == 0 or die "Dump sub node failed in ${test_name}";

    # compare dumped results
    is(system("diff "
    . $pub_dump . " "
    . $sub_dump), 0, "Dumped results diff in ${test_name}");
}

sub init_node {
    my $node_name = $_[0];
    my $node = PostgreSQL::Test::Cluster->new($node_name);
    $node->init;
    # increase some settings that Cluster->new makes too low by default.
    $node->adjust_conf('postgresql.conf', 'max_connections', '25');
    $node->append_conf('postgresql.conf',
		   'max_prepared_transactions = 10');
    return $node;
}

sub init_pub_node {
    my $node_name = $_[0]."_pub";
    return init_node($node_name)
}

sub init_sub_node {
    my $node_name = $_[0]."_sub";
    return init_node($node_name)
}

sub create_deparse_testing_resources_on_pub_node {
    my $node = $_[0];
    my $dbname = $_[1];
    $node -> psql($dbname, q(
        begin;
        CREATE EXTENSION test_ddl_deparse_regress;
        create table deparsed_ddls(id SERIAL PRIMARY KEY, tag text, object_identity text, ddl text);

        create or replace function deparse_to_json()
            returns event_trigger language plpgsql as
        $$
        declare
            r record;
        begin
            for r in select * from pg_event_trigger_ddl_commands()
            loop
                insert into deparsed_ddls(tag, object_identity, ddl) values (r.command_tag, r.object_identity, pg_catalog.ddl_deparse_to_json(r.command));
            end loop;
        END;
        $$;

        create or replace function deparse_drops_to_json()
            returns event_trigger language plpgsql as
        $$
        declare
            r record;
        begin
            for r in select * from pg_event_trigger_dropped_objects()
            loop
                insert into deparsed_ddls(tag, object_identity, ddl) values (r.object_type, r.object_identity, public.deparse_drop_ddl(r.object_identity, r.object_type));
            end loop;
        END;
        $$;

        create event trigger ddl_deparse_trig
        on ddl_command_end execute procedure deparse_to_json();

        create event trigger ddl_drops_deparse_trig
        on sql_drop execute procedure deparse_drops_to_json();

        commit;
    ));
}

sub clean_deparse_testing_resources_on_pub_node {
    my $node = $_[0];
    my $dbname = $_[1];
    # Drop the event trigger and the function before taking a logical dump.
    $node -> safe_psql($dbname,q(
        drop event trigger ddl_deparse_trig;
        drop event trigger ddl_drops_deparse_trig;
        drop function deparse_to_json();
        drop function deparse_drops_to_json();
        drop table deparsed_ddls;
        DROP EXTENSION test_ddl_deparse_regress;
    ));
}

sub trim {
    my @out = @_;
    for (@out) {
        s/^\s+//;
        s/\s+$//;
    }
    return wantarray ? @out : $out[0];
}

# Create and start pub sub nodes
my $pub_node = init_pub_node("test");
my $sub_node = init_sub_node("test");
my $dbname = "postgres";
$pub_node -> start;
$sub_node -> start;

# load test cases from the regression tests
my @regress_tests = split /\s+/, $ENV{REGRESS};

foreach(@regress_tests) {
    my $test_name = trim($_);
    # skip if it's regression test preparation or empty string
    if ($test_name eq "" or $test_name eq "test_ddl_deparse")
    {
        next;
    }
    eval {execute_test_case($test_name, $pub_node, $sub_node, $dbname);};
    if ($@ ne "")
    {
        fail($@);
    }
}
close;

# Close nodes
$pub_node->stop;
$sub_node->stop;

done_testing();
