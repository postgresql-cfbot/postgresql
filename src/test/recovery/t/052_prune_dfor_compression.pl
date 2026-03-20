use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# ------------------------------------------------------------
# Workload generating dead tuples and PRUNE WAL
# ------------------------------------------------------------
sub generate_prune_workload
{
	my ($node, $workload) = @_;


	my $start_lsn;
	my $end_lsn;

	if ($workload eq "vacuum_with_index"
		|| $workload eq "vacuum_no_index")
	{
		$node->safe_psql('postgres', q{
			CREATE TABLE t_prune (
				id int,
				val text
			) WITH (fillfactor = 100, autovacuum_enabled = false);
		});

		$node->safe_psql('postgres', q{
			SET vacuum_freeze_min_age = 0;
			SET vacuum_freeze_table_age = 0;
		});

		# -------------------------
		# Phase 1: INSERT
		# -------------------------
		$node->safe_psql('postgres', q{
			INSERT INTO t_prune
			SELECT g, 'x'
			FROM generate_series(1,3000000) g;
		});

		# Optional index
		if ($workload eq "vacuum_with_index")
		{
			$node->safe_psql('postgres', q{
				CREATE INDEX ON t_prune(id);
			});
		}
		# -------------------------
		# Phase 2: DELETE + VACUUM
		# -------------------------
		$node->safe_psql('postgres', q{
			DELETE FROM t_prune
			WHERE id % 500 <> 0;
		});

		# Force WAL flush and capture LSN
		$start_lsn = $node->safe_psql('postgres', q{
			SELECT pg_current_wal_flush_lsn();
		});

		# VACUUM cycles to trigger PRUNE
		for my $i (1..3)
		{
			$node->safe_psql('postgres', q{ VACUUM FREEZE t_prune; });
		}

		$end_lsn = $node->safe_psql('postgres', q{
			SELECT pg_current_wal_flush_lsn();
		});
	}
	else
	{
		die "Workload is not defined: workload=$workload";
	}

	chomp($start_lsn);
	print "Captured start LSN: $start_lsn\n";
	chomp($end_lsn);
	print "Captured end LSN: $end_lsn\n";

	return ($start_lsn, $end_lsn);
}

# ------------------------------------------------------------
# WAL analyzer
# ------------------------------------------------------------
sub collect_wal_stats
{
	my ($node, $start_lsn, $end_lsn) = @_;

	my $wal_dir = $node->data_dir . "/pg_wal";

	print "wal_dir=" . $wal_dir . "\n";
	print "collect_wal_stats: start_lsn=$start_lsn\n";
	print "collect_wal_stats: end_lsn=$end_lsn\n";

	my $cmd;

	if (defined $end_lsn && $end_lsn ne '')
	{
		$cmd = "pg_waldump -p $wal_dir -s $start_lsn -e $end_lsn 2>/dev/null";
	}
	else
	{
		$cmd = "pg_waldump -p $wal_dir -s $start_lsn 2>/dev/null";
	}

	my @lines = `$cmd`;

	# -------------------------
	# Counters
	# -------------------------
	my $total_records = 0;
	my $total_bytes   = 0;

	my $prune_records = 0;
	my $prune_bytes   = 0;

	foreach my $line (@lines)
	{
		# Extract total record size
		if ($line =~ /len \(rec\/tot\):\s*\d+\/\s*(\d+)/)
		{
			my $size = $1;

			$total_records++;
			$total_bytes += $size;

			# PRUNE-specific tracking
			if ($line =~ /PRUNE_VACUUM_SCAN/)
			{
				$prune_records++;
				$prune_bytes += $size;
			}
		}
	}

	if ($total_records == 0)
	{
		die "No WAL records found in range $start_lsn → $end_lsn";
	}

	print "TOTAL: records=$total_records; bytes=$total_bytes\n";
	print "PRUNE: records=$prune_records; bytes=$prune_bytes\n";

	return {
		total_records => $total_records,
		total_bytes   => $total_bytes,
		prune_records => $prune_records,
		prune_bytes   => $prune_bytes,
	};
}

# ------------------------------------------------------------
# Run test on a fresh cluster
# ------------------------------------------------------------
sub run_cluster_test
{
	my ($name, $compression, $workload) = @_;

	my $node = PostgreSQL::Test::Cluster->new($name);

	$node->init;

	$node->append_conf('postgresql.conf', qq{
		wal_level = replica
		autovacuum = off
		wal_prune_dfor_compression = $compression
 		wal_keep_size = '1GB'
		max_wal_size = '20GB'
	});

	$node->start;

	my ($start_lsn, $end_lsn) = generate_prune_workload($node, $workload);

	$node->stop;

	return collect_wal_stats($node, $start_lsn, $end_lsn);
}

# ------------------------------------------------------------
# Formatting helpers
# ------------------------------------------------------------

sub _pct_reduction
{
	my ($before, $after) = @_;
	return "N/A" if $before == 0;

	my $pct = 100 * ($before - $after) / $before;
	return sprintf("%d%%", int($pct + 0.5));
}

sub _ratio
{
	my ($before, $after) = @_;
	return "N/A" if $after == 0;

	my $r = $before / $after;
	return sprintf("%.1fx", $r);
}

# ------------------------------------------------------------
# Report: total WAL stats
# ------------------------------------------------------------
sub report_wal_diff
{
	my ($off, $on) = @_;

	my $b_bytes = $off->{total_bytes};
	my $a_bytes = $on->{total_bytes};

	printf "%-20s %17s %17s %11s\n",
		"-" x 20, "-" x 17, "-" x 17, "-" x 11;

	printf "%-20s %17s %17s %11s\n",
		"", "DFOR off, bytes", "DFOR on, bytes", "Reduction";

	printf "%-20s %17s %17s %11s\n",
		"-" x 20, "-" x 17, "-" x 17, "-" x 11;

	printf "%-20s %17d %17d %11s\n",
		"WAL total size",
		$off->{total_bytes},
		$on->{total_bytes},
		_pct_reduction($off->{total_bytes}, $on->{total_bytes});

	printf "%-20s %17d %17d %11s\n\n",
		"Prune records size",
		$off->{prune_bytes},
		$on->{prune_bytes},
		_ratio($off->{prune_bytes}, $on->{prune_bytes});
}

# ------------------------------------------------------------
# Scenario 1: VACUUM without index
# ------------------------------------------------------------
my $off_noidx = run_cluster_test("prune_off_noidx", "off", "vacuum_no_index");
my $on_noidx  = run_cluster_test("prune_on_noidx",  "on",  "vacuum_no_index");

cmp_ok(
	$off_noidx->{prune_bytes},
	'>',
	$on_noidx->{prune_bytes},
	'DFOR reduces the PRUNE WAL size on vacuuming a table having no index.'
);

cmp_ok(
	$off_noidx->{total_bytes},
	'>',
	$on_noidx->{total_bytes},
	'DFOR reduces the total WAL size on vacuuming a table having no index.'
);

print "\n\n=== VACUUM (table with no index) ===\n";
report_wal_diff($off_noidx, $on_noidx);

# ------------------------------------------------------------
# Scenario 2: VACUUM with index
# ------------------------------------------------------------
my $off_idx = run_cluster_test("prune_off_idx", "off", "vacuum_with_index");
my $on_idx  = run_cluster_test("prune_on_idx",  "on",  "vacuum_with_index");

cmp_ok(
	$off_idx->{prune_bytes},
	'>',
	$on_idx->{prune_bytes},
	'DFOR reduces the PRUNE WAL size on vacuuming a table having an index.'
);

cmp_ok(
	$off_idx->{total_bytes},
	'>=',
	$on_idx->{total_bytes},
	'DFOR reduces the total WAL size on vacuuming a table having an index.'
);

print "\n\n=== VACUUM (table with index) ===\n";
report_wal_diff($off_idx, $on_idx);

done_testing();
