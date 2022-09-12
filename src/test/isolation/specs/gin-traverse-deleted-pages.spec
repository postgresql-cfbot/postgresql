setup
{
  create table tmp (ar int[]) with (autovacuum_enabled = false);
  insert into tmp (select array[1] from generate_series(1,10000) i);
  insert into tmp values (array[1,2]);
  insert into tmp (select array[1] from generate_series(1,10000) i);
  create index tmp_idx on tmp using gin(ar);
  delete from tmp;
}

teardown
{
  DROP TABLE tmp;
}

session "s1"
setup		{ set enable_stopevents = true;
			  set max_parallel_workers_per_gather = 0; }
step "s1s"	{ select * from tmp where ar @> array[1,2]; }

session "s2"
step "s2s1"	{ select pg_stopevent_set('EntryFindPostingLeafPage',
									  '$.datname == "isolation_regression" && $.relname == "tmp_idx"'); }
step "s2v"	{ vacuum tmp; }
step "s2s2"	{ select pg_stopevent_set('ReleaseAndReadBuffer',
									  '$.datname == "isolation_regression" && $.relname == "tmp_idx"');
			  select pg_stopevent_reset('EntryFindPostingLeafPage'); }
step "s2s3"	{ select pg_stopevent_reset('ReleaseAndReadBuffer'); }

permutation "s2s1" "s1s" "s2s2" "s2v" "s2s3"
