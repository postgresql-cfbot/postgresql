/* ON ERROR behavior */

VALUES (CAST('error' AS integer));
VALUES (CAST('error' AS integer ERROR ON ERROR));
VALUES (CAST('error' AS integer NULL ON ERROR));
VALUES (CAST('error' AS integer DEFAULT 42 ON ERROR));

SELECT CAST('{123,abc,456}' AS integer[] DEFAULT '{-789}' ON ERROR) as array_test1;
SELECT CAST('{234,def,567}'::text[] AS integer[] DEFAULT '{-1011}' ON ERROR) as array_test2;

SELECT CAST(u.arg AS integer DEFAULT -1 ON ERROR) AS unnest_test1 FROM unnest('{345,ghi,678}'::text[]) AS u(arg);
