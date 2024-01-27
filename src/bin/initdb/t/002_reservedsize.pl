
# Copyright (c) 2024, PostgreSQL Global Development Group


use strict;
use warnings;
use Fcntl ':mode';
use File::stat qw{lstat};
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# validate expected handling of --reserved-size

# default is 0 reserved size
my $node1 = PostgreSQL::Test::Cluster->new('node1');
$node1->init();
$node1->start;

is($node1->safe_psql('postgres',q{SELECT current_setting('reserved_page_size')}),
   0, "reserved_size defaults to 0");

$node1->stop;

# reserve 8 bytes
my $node2 = PostgreSQL::Test::Cluster->new('node2');
$node2->init(extra => ['--reserved-size=8'] );
$node2->start;

is($node2->safe_psql('postgres',q{SELECT current_setting('reserved_page_size')}),
   8, "reserved_page_size passes through correctly");

$node2->stop;

# reserve non-multiple of 8 bytes : initdb error
command_fails([ 'initdb', '--reserved-size=18' ],
	'multiple');

# reserve too much space : initdb error
command_fails([ 'initdb', '--reserved-size=1024' ],
	'multiple');

done_testing();
