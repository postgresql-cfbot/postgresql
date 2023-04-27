CREATE TABLE drs_test1 (a int);
INSERT INTO drs_test1 VALUES (1), (2), (3);
CREATE TABLE drs_test2 (x text, y text);
INSERT INTO drs_test2 VALUES ('abc', 'def'), ('foo', 'bar');


CREATE PROCEDURE pdrstest1(x int)
LANGUAGE plpgsql
DYNAMIC RESULT SETS 2
AS $$
DECLARE
  c1 CURSOR WITH RETURN (y int) FOR SELECT a * y AS ay FROM drs_test1;
  c2 CURSOR WITH RETURN FOR SELECT * FROM drs_test2;
BEGIN
  OPEN c1(x);
  IF x > 1 THEN
    OPEN c2;
  END IF;
END;
$$;

CALL pdrstest1(1);
CALL pdrstest1(2);


DO $$
DECLARE
  c1 CURSOR WITH RETURN (y int) FOR SELECT a * y AS ay FROM drs_test1;
  c2 CURSOR WITH RETURN FOR SELECT * FROM drs_test2;
BEGIN
  OPEN c1(1);
  OPEN c2;
END;
$$;


-- (The result sets of the called procedure are not returned.)
DO $$
BEGIN
  CALL pdrstest1(1);
END;
$$;


CREATE PROCEDURE pdrstest2(x int)
LANGUAGE plpgsql
DYNAMIC RESULT SETS 2
AS $$
DECLARE
  c1 refcursor;
  c2 refcursor;
BEGIN
  OPEN c1 WITH RETURN FOR SELECT a * x AS ax FROM drs_test1;
  IF x > 1 THEN
    OPEN c2 SCROLL WITH RETURN FOR SELECT * FROM drs_test2;
  END IF;
END;
$$;

CALL pdrstest2(1);
CALL pdrstest2(2);


DROP TABLE drs_test1, drs_test2;
