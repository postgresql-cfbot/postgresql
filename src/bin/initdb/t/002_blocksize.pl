
# Copyright (c) 2023, PostgreSQL Global Development Group


use strict;
use warnings;
use Fcntl ':mode';
use File::stat qw{lstat};
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

for my $blocksize (1,2,4,8,16,32) {
	my $node1 = PostgreSQL::Test::Cluster->new('node' . $blocksize);
	$node1->init(extra => ['--block-size='.($blocksize)] );
	$node1->start;

	is($node1->safe_psql('postgres',q{SELECT current_setting('block_size')}),
	   1024 * $blocksize, "initdb with blocksize " . $blocksize . "k");

	$node1->stop;
}

done_testing();
