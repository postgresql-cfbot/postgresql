
# Copyright (c) 2021-2022, PostgreSQL Global Development Group

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

program_help_ok('pg_waldump');
program_version_ok('pg_waldump');
program_options_handling_ok('pg_waldump');

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;

my $wal_dump_path = $node->data_dir . "/pg_wal/000000010000000000000001";
my ($stdout, $stderr);

# test pg_waldump
IPC::Run::run [ 'pg_waldump', "$wal_dump_path" ], '>', \$stdout, '2>', \$stderr;
isnt($stdout, '', "");

# test pg_waldump with -p (path), -s (start), -e (end) options
IPC::Run::run [ 'pg_waldump', "$wal_dump_path", '-s', '0/0132C160', '-e', '0/018C41A0' ], '>', \$stdout, '2>', \$stderr;
isnt($stdout, '', "");

# test pg_waldump with -p (path), -s (start), -e (end) options -z (stats)
IPC::Run::run [ 'pg_waldump', "$wal_dump_path", '-s', '0/0132C160', '-e', '0/018C41A0', '-z' ], '>', \$stdout, '2>', \$stderr;
isnt($stdout, '', "");

# test pg_waldump with -F (main)
IPC::Run::run [ 'pg_waldump', "$wal_dump_path", '-F', 'main' ], '>', \$stdout, '2>', \$stderr;
isnt($stdout, '', "");

# test pg_waldump with -F (fsm)
IPC::Run::run [ 'pg_waldump', "$wal_dump_path", '-F', 'fsm' ], '>', \$stdout, '2>', \$stderr;
is($stdout, '', "");

# test pg_waldump with -F (vm)
IPC::Run::run [ 'pg_waldump', "$wal_dump_path", '-F', 'vm' ], '>', \$stdout, '2>', \$stderr;
isnt($stdout, '', "");

# test pg_waldump with -F (init)
IPC::Run::run [ 'pg_waldump', "$wal_dump_path", '-F', 'init' ], '>', \$stdout, '2>', \$stderr;
is($stdout, '', "");

# test pg_waldump with -q
IPC::Run::run [ 'pg_waldump', "$wal_dump_path", '-F', 'init' ], '>', \$stdout, '2>', \$stderr;
is($stdout, '', "");
done_testing();

