
# Copyright (c) 2021, PostgreSQL Global Development Group

# Test relation persistence change
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Test::More tests => 30;
use IPC::Run qw(pump finish timer);
use Config;

my $data_unit = 2000;

# Initialize primary node.
my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
# we don't want checkpointing
$node->append_conf('postgresql.conf', qq(
checkpoint_timeout = '24h'
));
$node->start;
create($node);

my $relfilenodes1 = relfilenodes();

# correctly recover empty tables
$node->stop('immediate');
$node->start;
insert($node, 0, $data_unit, 0);

# data persists after a crash
$node->stop('immediate');
$node->start;
checkdataloss($data_unit, 'crash logged 1');

set_unlogged($node);
# SET UNLOGGED shouldn't change relfilenode
my $relfilenodes2 = relfilenodes();
checkrelfilenodes($relfilenodes1, $relfilenodes2, 'logged->unlogged');

# data cleanly vanishes after a crash
$node->stop('immediate');
$node->start;
checkdataloss(0, 'crash unlogged');

insert($node, 0, $data_unit, 0);
set_logged($node);

$node->stop('immediate');
$node->start;
# SET LOGGED shouldn't change relfilenode and data should survive the crash
my $relfilenodes3 = relfilenodes();
checkrelfilenodes($relfilenodes2, $relfilenodes3, 'unlogged->logged');
checkdataloss($data_unit, 'crash logged 2');

# unlogged insert -> graceful stop
set_unlogged($node);
insert($node, $data_unit, $data_unit, 0);
$node->stop;
$node->start;
checkdataloss($data_unit * 2, 'unlogged graceful restart');

# crash during transaction
set_logged($node);
$node->stop('immediate');
$node->start;
insert($node, $data_unit * 2, $data_unit, 0);

my $h;

# insert(,,,1) requires IO::Pty. Skip the test if the module is not
# available, but do the insert to make the expected situation for the
# later tests.
eval { require IO::Pty; };
if ($@)
{
	insert($node, $data_unit * 3, $data_unit, 0);
	ok (1, 'SKIPPED: IO::Pty is needed');
	ok (1, 'SKIPPED: IO::Pty is needed');
}
else
{
	$h = insert($node, $data_unit * 3, $data_unit, 1); ## this is aborted
}

$node->stop('immediate');

# finishing $h stalls this case, just tear it off.
$h = undef;

# check if indexes are working
$node->start;
# drop first half of data to reduce run time
$node->safe_psql('postgres', 'DELETE FROM t WHERE bt < ' . $data_unit * 2);
check($node, $data_unit * 2, $data_unit * 3 - 1, 'final check');

sub create
{
	my ($node) = @_;

	$node->psql('postgres', qq(
			CREATE TABLE t (bt int, gin int[], gist point, hash int,
				brin int, spgist point);
			CREATE INDEX i_bt ON t USING btree (bt);
			CREATE INDEX i_gin ON t USING gin (gin);
			CREATE INDEX i_gist ON t USING gist (gist);
			CREATE INDEX i_hash ON t USING hash (hash);
			CREATE INDEX i_brin ON t USING brin (brin);
			CREATE INDEX i_spgist ON t USING spgist (spgist);));
}


sub insert
{
	my ($node, $st, $num, $interactive) = @_;
	my $ed = $st + $num - 1;
	my $query = qq(BEGIN;
INSERT INTO t
 (SELECT i, ARRAY[i, i * 2], point(i, i * 2), i, i, point(i, i)
  FROM generate_series($st, $ed) i);
);

	if ($interactive)
	{
		my $in  = '';
		my $out = '';
		my $timer = timer(10);

		my $h = $node->interactive_psql('postgres', \$in, \$out, $timer);
		like($out, qr/psql/, "print startup banner");

		$in .= "$query\n";
		pump $h until ($out =~ /[\n\r]+INSERT 0 $num[\n\r]+/ ||
					   $timer->is_expired);
		ok(($out =~ /[\n\r]+INSERT 0 $num[\n\r]+/), "inserted-$st-$num");
		return $h
		# the trasaction is not terminated
	}
	else
	{
		$node->psql('postgres', $query . "COMMIT;");
		return undef;
	}
}

sub check
{
	my ($node, $st, $ed, $head) = @_;
	my $num_data = $ed - $st + 1;

	is($node->safe_psql('postgres', qq(
			SET enable_seqscan TO true;
			SET enable_indexscan TO false;
			SELECT COUNT(*) FROM t, generate_series($st, $ed) i
			WHERE bt = i)),
	   $num_data, "$head: heap is not broken");
	is($node->safe_psql('postgres', qq(
			SET enable_seqscan TO false;
			SET enable_indexscan TO true;
			SELECT COUNT(*) FROM t, generate_series($st, $ed) i
			WHERE bt = i)),
	   $num_data, "$head: btree is not broken");
	is($node->safe_psql('postgres', qq(
			SET enable_seqscan TO false;
			SET enable_indexscan TO true;
			SELECT COUNT(*) FROM t, generate_series($st, $ed) i
			WHERE gin = ARRAY[i, i * 2];)),
	   $num_data, "$head: gin is not broken");
	is($node->safe_psql('postgres', qq(
			SET enable_seqscan TO false;
			SET enable_indexscan TO true;
			SELECT COUNT(*) FROM t, generate_series($st, $ed) i
			WHERE gist <@ box(point(i-0.5, i*2-0.5),point(i+0.5, i*2+0.5));)),
	   $num_data, "$head: gist is not broken");
	is($node->safe_psql('postgres', qq(
			SET enable_seqscan TO false;
			SET enable_indexscan TO true;
			SELECT COUNT(*) FROM t, generate_series($st, $ed) i
			WHERE hash = i;)),
	   $num_data, "$head: hash is not broken");
	is($node->safe_psql('postgres', qq(
			SET enable_seqscan TO false;
			SET enable_indexscan TO true;
			SELECT COUNT(*) FROM t, generate_series($st, $ed) i
			WHERE brin = i;)),
	   $num_data, "$head: brin is not broken");
	is($node->safe_psql('postgres', qq(
			SET enable_seqscan TO false;
			SET enable_indexscan TO true;
			SELECT COUNT(*) FROM t, generate_series($st, $ed) i
			WHERE spgist <@ box(point(i-0.5,i-0.5),point(i+0.5,i+0.5));)),
	   $num_data, "$head: spgist is not broken");
}

sub set_unlogged
{
	my ($node) = @_;

	$node->psql('postgres', qq(
			ALTER TABLE t SET UNLOGGED;
));
}

sub set_logged
{
	my ($node) = @_;

	$node->psql('postgres', qq(
			ALTER TABLE t SET LOGGED;
));
}

sub relfilenodes
{
	my $result = $node->safe_psql('postgres', qq{
		SELECT relname, relfilenode FROM pg_class
		WHERE relname
		IN ('t', 'i_bt','i_gin','i_gist','i_hash','i_brin','i_spgist');});

	my %relfilenodes;

	foreach my $l (split(/\n/, $result))
	{
		die "unexpected format: $l" if ($l !~ /^([^|]+)\|([0-9]+)$/);
		$relfilenodes{$1} = $2;
	}

	# the number must correspond to the in list above
	is (scalar %relfilenodes, 7, "number of relations is correct");

	return \%relfilenodes;
}

sub checkrelfilenodes
{
	my ($rnodes1, $rnodes2, $s) = @_;

	foreach my $n (keys %{$rnodes1})
	{
		if ($n eq 'i_gist')
		{
			# persistence of GiST index is not changed in-place
			isnt($rnodes1->{$n}, $rnodes2->{$n},
				 "$s: relfilenode is changed: $n");
		}
		else
		{
			# otherwise all relations are processed in-place
			is($rnodes1->{$n}, $rnodes2->{$n},
				 "$s: relfilenode is not changed: $n");
		}
	}
}

sub checkdataloss
{
	my ($expected, $s) = @_;

	is($node->safe_psql('postgres', "SELECT count(*) FROM t;"), $expected,
	   "$s: data in table t is in the expected state");
}
