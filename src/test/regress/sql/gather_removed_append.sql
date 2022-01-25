-- Test correctness of parallel query execution after removal
-- of Append path due to single non-trivial child.

DROP TABLE IF EXISTS gather_append_1, gather_append_2;

CREATE TABLE gather_append_1 (
    fk int,
    f bool
);

INSERT INTO gather_append_1 (fk, f) SELECT i, i%50=0 from generate_series(1, 2000) as i;

CREATE INDEX gather_append_1_ix on gather_append_1 (f);

CREATE TABLE gather_append_2 (
    fk int,
    val serial
);

INSERT INTO gather_append_2 (fk) SELECT fk from gather_append_1, generate_series(1, 5) as i;

ANALYZE gather_append_1, gather_append_2;

SET max_parallel_workers_per_gather = 0;

-- Find correct rows count
SELECT count(1)
FROM (
  SELECT fk FROM gather_append_1 WHERE f
  UNION ALL
  SELECT fk FROM gather_append_1 WHERE false
) as t
LEFT OUTER JOIN gather_append_2
USING (fk);

SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0.1;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 2;

SELECT count(1)
FROM (
  SELECT fk FROM gather_append_1 WHERE f
  UNION ALL
  SELECT fk FROM gather_append_1 WHERE false
) as t
LEFT OUTER JOIN gather_append_2
USING (fk);

-- The buckets/batches/memory values from the Parallel Hash node can vary between
-- machines.  Let's just replace the number with an 'N'.
create function explain_gather(query text) returns setof text
language plpgsql as
$$
declare
    ln text;
begin
    for ln in
        execute format('explain (analyze, costs off, summary off, timing off, verbose off, buffers off) %s',
            query)
    loop
        if not ln like '%Gather%' then
            ln := regexp_replace(ln, 'actual rows=\d+ loops=\d+', 'actual rows=R loops=L');
        end if;
        ln := regexp_replace(ln, 'Buckets: \d+', 'Buckets: N');
        ln := regexp_replace(ln, 'Batches: \d+', 'Batches: N');
        ln := regexp_replace(ln, 'Memory Usage: \d+', 'Memory Usage: N');
        return next ln;
    end loop;
end;
$$;

-- Result rows in root node should be equal to non-parallel count
SELECT explain_gather('
SELECT val
FROM (
  SELECT fk FROM gather_append_1 WHERE f
  UNION ALL
  SELECT fk FROM gather_append_1 WHERE false
) as t
LEFT OUTER JOIN gather_append_2
USING (fk);');

-- Result rows in root node should be equal to non-parallel count
SELECT explain_gather('
SELECT val
FROM (
  SELECT fk FROM gather_append_1 WHERE f
  UNION ALL
  SELECT fk FROM gather_append_1 WHERE false
) as t
LEFT OUTER JOIN gather_append_2
USING (fk)
ORDER BY val;');
