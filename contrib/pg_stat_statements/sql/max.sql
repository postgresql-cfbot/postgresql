SHOW pg_stat_statements.max;

SET pg_stat_statements.track = 'all';

DO $$
BEGIN
  FOR i IN 1..101 LOOP
    EXECUTE format('create table t%s (a int)', lpad(i::text, 3, '0'));
  END LOOP;
END
$$;

SELECT pg_stat_statements_reset() IS NOT NULL AS t;

DO $$
BEGIN
  FOR i IN 1..98 LOOP
    EXECUTE format('select * from t%s', lpad(i::text, 3, '0'));
  END LOOP;
END
$$;

SELECT count(*) <= 100 AND count(*) > 0 FROM pg_stat_statements;
SELECT query FROM pg_stat_statements WHERE query LIKE '%t001%' OR query LIKE '%t098%' ORDER BY query;

DO $$
BEGIN
  FOR i IN 2..98 LOOP
    EXECUTE format('select * from t%s', lpad(i::text, 3, '0'));
  END LOOP;
END
$$;

DO $$
BEGIN
  FOR i IN 99..100 LOOP
    EXECUTE format('select * from t%s', lpad(i::text, 3, '0'));
  END LOOP;
END
$$;

SELECT count(*) <= 100 AND count(*) > 0 FROM pg_stat_statements;
-- record for t001 has been kicked out
SELECT query FROM pg_stat_statements WHERE query LIKE '%t001%' ORDER BY query;
