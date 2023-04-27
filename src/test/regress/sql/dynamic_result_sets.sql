CREATE TABLE drs_test1 (a int);
INSERT INTO drs_test1 VALUES (1), (2), (3);
CREATE TABLE drs_test2 (x text, y text);
INSERT INTO drs_test2 VALUES ('abc', 'def'), ('foo', 'bar');


-- return a couple of result sets from a procedure

CREATE PROCEDURE pdrstest1()
LANGUAGE SQL
DYNAMIC RESULT SETS 2
AS $$
DECLARE c1 CURSOR WITH RETURN FOR SELECT * FROM drs_test1;
DECLARE c2 CURSOR WITH RETURN FOR SELECT * FROM drs_test2;
$$;

CALL pdrstest1();
CALL pdrstest1() \bind \g


-- return too many result sets from a procedure

CREATE PROCEDURE pdrstest2()
LANGUAGE SQL
DYNAMIC RESULT SETS 1
AS $$
DECLARE c1 CURSOR WITH RETURN FOR SELECT * FROM drs_test1;
DECLARE c2 CURSOR WITH RETURN FOR SELECT * FROM drs_test2;
$$;

CALL pdrstest2();
CALL pdrstest2() \bind \g


-- nested calls

CREATE PROCEDURE pdrstest3()
LANGUAGE SQL
DYNAMIC RESULT SETS 1
AS $$
CALL pdrstest1();
DECLARE c3 CURSOR WITH RETURN FOR SELECT * FROM drs_test1 WHERE a < 2;
$$;

-- (The result sets of the called procedure are not returned.)
CALL pdrstest3();
CALL pdrstest3() \bind \g


-- both out parameter and result sets

CREATE PROCEDURE pdrstest4(INOUT a text)
LANGUAGE SQL
DYNAMIC RESULT SETS 1
AS $$
DECLARE c4 CURSOR WITH RETURN FOR SELECT * FROM drs_test1;
SELECT a || a;
$$;

CALL pdrstest4('x');
CALL pdrstest4($1) \bind 'y' \g


-- test the nested error handling

CREATE TABLE drs_test_dummy (a int);

CREATE PROCEDURE pdrstest5a()
LANGUAGE SQL
DYNAMIC RESULT SETS 1
AS $$
DECLARE c5a CURSOR WITH RETURN FOR SELECT * FROM drs_test_dummy;
$$;

CREATE PROCEDURE pdrstest5b()
LANGUAGE SQL
DYNAMIC RESULT SETS 1
AS $$
CALL pdrstest5a();
$$;

DROP TABLE drs_test_dummy;

CALL pdrstest5b();
CALL pdrstest5b() \bind \g


-- cleanup

DROP TABLE drs_test1, drs_test2;
