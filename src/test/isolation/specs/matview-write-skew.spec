# Test write skew with a materialized view.
#
# This test has two serializable transactions: one which refreshes a
# materialized view containing summary of the order information
# (suppose that is performed once per day), and one which generates
# an order whose record date is determined by referring to the last
# One must be rolled back to prevent the write skew anomaly.
#
# Any overlap between the transactions must cause a serialization failure.

setup
{
  CREATE TABLE orders (date date, item text, num int);
  INSERT INTO orders VALUES ('2022-04-01', 'apple', 10), ('2022-04-01', 'banana', 20);

  CREATE MATERIALIZED VIEW order_summary AS
    SELECT date, item, sum(num) FROM orders GROUP BY date, item;;
  CREATE UNIQUE INDEX ON order_summary(date, item);

  INSERT INTO orders VALUES ('2022-04-02', 'apple', 20);
}

teardown
{
  DROP MATERIALIZED VIEW order_summary;
  DROP TABLE orders;
}

session s1
setup { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step rxwy1 { REFRESH MATERIALIZED VIEW CONCURRENTLY order_summary; }
step c1 { COMMIT; }

session s2
setup { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step rywx2 {
  DO $$DECLARE today date;
  BEGIN
    SELECT INTO today max(date) + 1 FROM order_summary;
    INSERT INTO orders VALUES (today, 'banana', 10);
  END$$;
}
step c2 { COMMIT; }
