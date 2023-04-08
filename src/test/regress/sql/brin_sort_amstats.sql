set enable_indexam_stats = true;

-- function to verify various sort-related data (total rows, ordering)
create or replace function brinsort_check_ordering(p_sql text, p_rows_expected int, p_desc boolean) returns void as $$
declare
    v_curs refcursor;
    v_row record;
    v_prev record;
    v_brin_sort_found bool := false;
    v_count int := 0;
begin

    -- needed because the p_sql query has different data types
    execute 'discard plans';

    OPEN v_curs NO SCROLL FOR EXECUTE format('explain %s', p_sql);

    LOOP
        FETCH v_curs INTO v_row;

        IF NOT FOUND THEN
            EXIT;
        END IF;

        IF v_row::text LIKE '%BRIN Sort%' THEN
            v_brin_sort_found := true;
            EXIT;
        END IF;
    END LOOP;

    CLOSE v_curs;

    IF NOT v_brin_sort_found THEN
        RAISE EXCEPTION 'BRIN Sort: not found';
    END IF;

    OPEN v_curs NO SCROLL FOR EXECUTE format(p_sql);

    LOOP
        FETCH v_curs INTO v_row;

        IF NOT FOUND THEN
            EXIT;
        END IF;

        IF v_prev IS NOT NULL THEN
            IF v_prev.val > v_row.val AND NOT p_desc THEN
                RAISE EXCEPTION 'ordering mismatch % > % (asc)', v_prev.val, v_row.val;
            END IF;
            IF v_prev.val < v_row.val AND p_desc THEN
                RAISE EXCEPTION 'ordering mismatch % < % (desc)', v_prev.val, v_row.val;
            END IF;
        END IF;

        v_prev := v_row;
        v_count := v_count + 1;
    END LOOP;

    CLOSE v_curs;

    IF v_count != p_rows_expected THEN
        RAISE EXCEPTION 'count mismatch: % != %', v_count, p_rows_expected;
    END IF;

end;
$$ language plpgsql;

create table brin_sort_test (int_val int, bigint_val bigint, text_val text, inet_val inet) with (fillfactor=10);

-- sequential values
insert into brin_sort_test
select
	i,
	-i,	-- same as int, but at least opposite
	lpad(i::text || md5(i::text), 40, '0'),
	'10.0.0.0'::inet + i
from generate_series(1,1000) s(i);

-- create brin indexes on individual columns
create index brin_sort_test_int_idx on brin_sort_test using brin (int_val) with (pages_per_range=1);
create index brin_sort_test_bigint_idx on brin_sort_test using brin (bigint_val) with (pages_per_range=1);
create index brin_sort_test_text_idx on brin_sort_test using brin (text_val) with (pages_per_range=1);
create index brin_sort_test_inet_idx on brin_sort_test using brin (inet_val inet_minmax_ops) with (pages_per_range=1);

-- 
vacuum analyze brin_sort_test;

set enable_seqscan = off;

select brinsort_check_ordering('select int_val as val from brin_sort_test order by int_val', 1000, false);
select brinsort_check_ordering('select int_val as val from brin_sort_test order by int_val desc', 1000, true);

select brinsort_check_ordering('select int_val as val from brin_sort_test order by int_val limit 100', 100, false);
select brinsort_check_ordering('select int_val as val from brin_sort_test order by int_val desc limit 100', 100, true);

select brinsort_check_ordering('select int_val as val from brin_sort_test order by int_val limit 100 offset 100', 100, false);
select brinsort_check_ordering('select int_val as val from brin_sort_test order by int_val desc limit 100 offset 100', 100, true);


select brinsort_check_ordering('select bigint_val as val from brin_sort_test order by bigint_val', 1000, false);
select brinsort_check_ordering('select bigint_val as val from brin_sort_test order by bigint_val desc', 1000, true);

select brinsort_check_ordering('select bigint_val as val from brin_sort_test order by bigint_val limit 100', 100, false);
select brinsort_check_ordering('select bigint_val as val from brin_sort_test order by bigint_val desc limit 100', 100, true);

select brinsort_check_ordering('select bigint_val as val from brin_sort_test order by bigint_val limit 100 offset 100', 100, false);
select brinsort_check_ordering('select bigint_val as val from brin_sort_test order by bigint_val desc limit 100 offset 100', 100, true);


select brinsort_check_ordering('select inet_val as val from brin_sort_test order by inet_val', 1000, false);
select brinsort_check_ordering('select inet_val as val from brin_sort_test order by inet_val desc', 1000, true);

select brinsort_check_ordering('select inet_val as val from brin_sort_test order by inet_val limit 100', 100, false);
select brinsort_check_ordering('select inet_val as val from brin_sort_test order by inet_val desc limit 100', 100, true);

select brinsort_check_ordering('select inet_val as val from brin_sort_test order by inet_val limit 100 offset 100', 100, false);
select brinsort_check_ordering('select inet_val as val from brin_sort_test order by inet_val desc limit 100 offset 100', 100, true);


select brinsort_check_ordering('select text_val as val from brin_sort_test order by text_val', 1000, false);
select brinsort_check_ordering('select text_val as val from brin_sort_test order by text_val desc', 1000, true);

select brinsort_check_ordering('select text_val as val from brin_sort_test order by text_val limit 100', 100, false);
select brinsort_check_ordering('select text_val as val from brin_sort_test order by text_val desc limit 100', 100, true);

select brinsort_check_ordering('select text_val as val from brin_sort_test order by text_val limit 100 offset 100', 100, false);
select brinsort_check_ordering('select text_val as val from brin_sort_test order by text_val desc limit 100 offset 100', 100, true);


-- semi-random data (sequential + randomness)
truncate table brin_sort_test;
insert into brin_sort_test
select
	i + (100 * random())::int,
	-(i + (100 * random())::int),	-- same as int, but at least opposite
	lpad((i + (100 * random())::int)::text || md5(i::text), 40, '0'),
	'10.0.0.0'::inet + (i + 100 * random()::int)
from generate_series(1,1000) s(i);

reindex table brin_sort_test;

vacuum analyze brin_sort_test;


select brinsort_check_ordering('select int_val as val from brin_sort_test order by int_val', 1000, false);
select brinsort_check_ordering('select int_val as val from brin_sort_test order by int_val desc', 1000, true);

select brinsort_check_ordering('select int_val as val from brin_sort_test order by int_val limit 100', 100, false);
select brinsort_check_ordering('select int_val as val from brin_sort_test order by int_val desc limit 100', 100, true);

select brinsort_check_ordering('select int_val as val from brin_sort_test order by int_val limit 100 offset 100', 100, false);
select brinsort_check_ordering('select int_val as val from brin_sort_test order by int_val desc limit 100 offset 100', 100, true);

 
select brinsort_check_ordering('select bigint_val as val from brin_sort_test order by bigint_val', 1000, false);
select brinsort_check_ordering('select bigint_val as val from brin_sort_test order by bigint_val desc', 1000, true);

select brinsort_check_ordering('select bigint_val as val from brin_sort_test order by bigint_val limit 100', 100, false);
select brinsort_check_ordering('select bigint_val as val from brin_sort_test order by bigint_val desc limit 100', 100, true);

select brinsort_check_ordering('select bigint_val as val from brin_sort_test order by bigint_val limit 100 offset 100', 100, false);
select brinsort_check_ordering('select bigint_val as val from brin_sort_test order by bigint_val desc limit 100 offset 100', 100, true);

 
select brinsort_check_ordering('select inet_val as val from brin_sort_test order by inet_val', 1000, false);
select brinsort_check_ordering('select inet_val as val from brin_sort_test order by inet_val desc', 1000, true);

select brinsort_check_ordering('select inet_val as val from brin_sort_test order by inet_val limit 100', 100, false);
select brinsort_check_ordering('select inet_val as val from brin_sort_test order by inet_val desc limit 100', 100, true);

select brinsort_check_ordering('select inet_val as val from brin_sort_test order by inet_val limit 100 offset 100', 100, false);
select brinsort_check_ordering('select inet_val as val from brin_sort_test order by inet_val desc limit 100 offset 100', 100, true);

 
select brinsort_check_ordering('select text_val as val from brin_sort_test order by text_val', 1000, false);
select brinsort_check_ordering('select text_val as val from brin_sort_test order by text_val desc', 1000, true);

select brinsort_check_ordering('select text_val as val from brin_sort_test order by text_val limit 100', 100, false);
select brinsort_check_ordering('select text_val as val from brin_sort_test order by text_val desc limit 100', 100, true);

select brinsort_check_ordering('select text_val as val from brin_sort_test order by text_val limit 100 offset 100', 100, false);
select brinsort_check_ordering('select text_val as val from brin_sort_test order by text_val desc limit 100 offset 100', 100, true);


-- random data
truncate table brin_sort_test;
insert into brin_sort_test
select
	(1000 * random())::int,
	-((1000 * random())::int),	-- same as int, but at least opposite
	lpad(((1000 * random())::int)::text || md5(i::text), 40, '0'),
	'10.0.0.0'::inet + (1000 * random()::int)
from generate_series(1,1000) s(i);

reindex table brin_sort_test;

vacuum analyze brin_sort_test;

 
select brinsort_check_ordering('select int_val as val from brin_sort_test order by int_val', 1000, false);
select brinsort_check_ordering('select int_val as val from brin_sort_test order by int_val desc', 1000, true);

select brinsort_check_ordering('select int_val as val from brin_sort_test order by int_val limit 100', 100, false);
select brinsort_check_ordering('select int_val as val from brin_sort_test order by int_val desc limit 100', 100, true);

select brinsort_check_ordering('select int_val as val from brin_sort_test order by int_val limit 100 offset 100', 100, false);
select brinsort_check_ordering('select int_val as val from brin_sort_test order by int_val desc limit 100 offset 100', 100, true);

 
select brinsort_check_ordering('select bigint_val as val from brin_sort_test order by bigint_val', 1000, false);
select brinsort_check_ordering('select bigint_val as val from brin_sort_test order by bigint_val desc', 1000, true);

select brinsort_check_ordering('select bigint_val as val from brin_sort_test order by bigint_val limit 100', 100, false);
select brinsort_check_ordering('select bigint_val as val from brin_sort_test order by bigint_val desc limit 100', 100, true);

select brinsort_check_ordering('select bigint_val as val from brin_sort_test order by bigint_val limit 100 offset 100', 100, false);
select brinsort_check_ordering('select bigint_val as val from brin_sort_test order by bigint_val desc limit 100 offset 100', 100, true);

 
select brinsort_check_ordering('select inet_val as val from brin_sort_test order by inet_val', 1000, false);
select brinsort_check_ordering('select inet_val as val from brin_sort_test order by inet_val desc', 1000, true);

select brinsort_check_ordering('select inet_val as val from brin_sort_test order by inet_val limit 100', 100, false);
select brinsort_check_ordering('select inet_val as val from brin_sort_test order by inet_val desc limit 100', 100, true);

select brinsort_check_ordering('select inet_val as val from brin_sort_test order by inet_val limit 100 offset 100', 100, false);
select brinsort_check_ordering('select inet_val as val from brin_sort_test order by inet_val desc limit 100 offset 100', 100, true);

 
select brinsort_check_ordering('select text_val as val from brin_sort_test order by text_val', 1000, false);
select brinsort_check_ordering('select text_val as val from brin_sort_test order by text_val desc', 1000, true);

select brinsort_check_ordering('select text_val as val from brin_sort_test order by text_val limit 100', 100, false);
select brinsort_check_ordering('select text_val as val from brin_sort_test order by text_val desc limit 100', 100, true);

select brinsort_check_ordering('select text_val as val from brin_sort_test order by text_val limit 100 offset 100', 100, false);
select brinsort_check_ordering('select text_val as val from brin_sort_test order by text_val desc limit 100 offset 100', 100, true);


drop table brin_sort_test;
