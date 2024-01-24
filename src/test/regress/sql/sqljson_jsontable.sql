-- JSON_TABLE

-- Should fail (JSON_TABLE can be used only in FROM clause)
SELECT JSON_TABLE('[]', '$');

-- Should fail (json argument not supported)
SELECT * FROM JSON_TABLE(NULL FORMAT JSON, '$' COLUMNS (foo text));

-- Should fail (no columns)
SELECT * FROM JSON_TABLE(NULL, '$' COLUMNS ());

SELECT * FROM JSON_TABLE (NULL::jsonb, '$' COLUMNS (v1 timestamp)) AS f (v1, v2);

-- NULL => empty table
SELECT * FROM JSON_TABLE(NULL::jsonb, '$' COLUMNS (foo int)) bar;

--
SELECT * FROM JSON_TABLE(jsonb '123', '$'
	COLUMNS (item int PATH '$', foo int)) bar;

-- JSON_TABLE: basic functionality
CREATE DOMAIN jsonb_test_domain AS text CHECK (value <> 'foo');
CREATE TEMP TABLE json_table_test (js) AS
	(VALUES
		('1'),
		('[]'),
		('{}'),
		('[1, 1.23, "2", "aaaaaaa", "foo", null, false, true, {"aaa": 123}, "[1,2]", "\"str\""]')
	);

-- Regular "unformatted" columns
SELECT *
FROM json_table_test vals
	LEFT OUTER JOIN
	JSON_TABLE(
		vals.js::jsonb, 'lax $[*]'
		COLUMNS (
			id FOR ORDINALITY,
			"int" int PATH '$',
			"text" text PATH '$',
			"char(4)" char(4) PATH '$',
			"bool" bool PATH '$',
			"numeric" numeric PATH '$',
			"domain" jsonb_test_domain PATH '$',
			js json PATH '$',
			jb jsonb PATH '$'
		)
	) jt
	ON true;

-- "formatted" columns
SELECT *
FROM json_table_test vals
	LEFT OUTER JOIN
	JSON_TABLE(
		vals.js::jsonb, 'lax $[*]'
		COLUMNS (
			id FOR ORDINALITY,
			jst text    FORMAT JSON  PATH '$',
			jsc char(4) FORMAT JSON  PATH '$',
			jsv varchar(4) FORMAT JSON  PATH '$',
			jsb jsonb FORMAT JSON PATH '$',
			jsbq jsonb FORMAT JSON PATH '$' OMIT QUOTES
		)
	) jt
	ON true;

-- EXISTS columns
SELECT *
FROM json_table_test vals
	LEFT OUTER JOIN
	JSON_TABLE(
		vals.js::jsonb, 'lax $[*]'
		COLUMNS (
			id FOR ORDINALITY,
			exists1 bool EXISTS PATH '$.aaa',
			exists2 int EXISTS PATH '$.aaa',
			exists3 int EXISTS PATH 'strict $.aaa' UNKNOWN ON ERROR,
			exists4 text EXISTS PATH 'strict $.aaa' FALSE ON ERROR
		)
	) jt
	ON true;

-- Other miscellanous checks
SELECT *
FROM json_table_test vals
	LEFT OUTER JOIN
	JSON_TABLE(
		vals.js::jsonb, 'lax $[*]'
		COLUMNS (
			id FOR ORDINALITY,
			aaa int, -- "aaa" has implicit path '$."aaa"'
			aaa1 int PATH '$.aaa',
			js2 json PATH '$',
			jsb2w jsonb PATH '$' WITH WRAPPER,
			jsb2q jsonb PATH '$' OMIT QUOTES,
			ia int[] PATH '$',
			ta text[] PATH '$',
			jba jsonb[] PATH '$'
		)
	) jt
	ON true;

-- JSON_TABLE: Test backward parsing

CREATE VIEW jsonb_table_view AS
SELECT * FROM
	JSON_TABLE(
		jsonb 'null', 'lax $[*]' PASSING 1 + 2 AS a, json '"foo"' AS "b c"
		COLUMNS (
			id FOR ORDINALITY,
			"int" int PATH '$',
			"text" text PATH '$',
			"char(4)" char(4) PATH '$',
			"bool" bool PATH '$',
			"numeric" numeric PATH '$',
			"domain" jsonb_test_domain PATH '$',
			js json PATH '$',
			jb jsonb PATH '$',
			jst text    FORMAT JSON  PATH '$',
			jsc char(4) FORMAT JSON  PATH '$',
			jsv varchar(4) FORMAT JSON  PATH '$',
			jsb jsonb   FORMAT JSON PATH '$',
			jsbq jsonb FORMAT JSON PATH '$' OMIT QUOTES,
			aaa int, -- implicit path '$."aaa"',
			aaa1 int PATH '$.aaa',
			exists1 bool EXISTS PATH '$.aaa',
			exists2 int EXISTS PATH '$.aaa' TRUE ON ERROR,
			exists3 text EXISTS PATH 'strict $.aaa' UNKNOWN ON ERROR,

			js2 json PATH '$',
			jsb2w jsonb PATH '$' WITH WRAPPER,
			jsb2q jsonb PATH '$' OMIT QUOTES,
			ia int[] PATH '$',
			ta text[] PATH '$',
			jba jsonb[] PATH '$',

			NESTED PATH '$[1]' AS p1 COLUMNS (
				a1 int,
				NESTED PATH '$[*]' AS "p1 1" COLUMNS (
					a11 text
				),
				b1 text
			),
			NESTED PATH '$[2]' AS p2 COLUMNS (
				NESTED PATH '$[*]' AS "p2:1" COLUMNS (
					a21 text
				),
				NESTED PATH '$[*]' AS p22 COLUMNS (
					a22 text
				)
			)
		)
	);

\sv jsonb_table_view

EXPLAIN (COSTS OFF, VERBOSE) SELECT * FROM jsonb_table_view;

-- JSON_TABLE() with alias
EXPLAIN (COSTS OFF, VERBOSE)
SELECT * FROM
	JSON_TABLE(
		jsonb 'null', 'lax $[*]' PASSING 1 + 2 AS a, json '"foo"' AS "b c"
		COLUMNS (
			id FOR ORDINALITY,
			"int" int PATH '$',
			"text" text PATH '$'
	)) json_table_func;

DROP VIEW jsonb_table_view;
DROP DOMAIN jsonb_test_domain;

-- JSON_TABLE: only one FOR ORDINALITY columns allowed
SELECT * FROM JSON_TABLE(jsonb '1', '$' COLUMNS (id FOR ORDINALITY, id2 FOR ORDINALITY, a int PATH '$.a' ERROR ON EMPTY)) jt;
SELECT * FROM JSON_TABLE(jsonb '1', '$' COLUMNS (id FOR ORDINALITY, a int PATH '$' ERROR ON EMPTY)) jt;

-- JSON_TABLE: ON EMPTY/ON ERROR behavior
SELECT *
FROM
	(VALUES ('1'), ('"err"')) vals(js),
	JSON_TABLE(vals.js::jsonb, '$' COLUMNS (a int PATH '$')) jt;

SELECT *
FROM
	(VALUES ('1'), ('"err"')) vals(js)
		LEFT OUTER JOIN
	JSON_TABLE(vals.js::jsonb, '$' COLUMNS (a int PATH '$') ERROR ON ERROR) jt
		ON true;

SELECT *
FROM
	(VALUES ('1'), ('"err"')) vals(js)
		LEFT OUTER JOIN
	JSON_TABLE(vals.js::jsonb, '$' COLUMNS (a int PATH '$' ERROR ON ERROR)) jt
		ON true;

SELECT * FROM JSON_TABLE(jsonb '1', '$' COLUMNS (a int PATH '$.a' ERROR ON EMPTY)) jt;
SELECT * FROM JSON_TABLE(jsonb '1', '$' COLUMNS (a int PATH 'strict $.a' ERROR ON EMPTY) ERROR ON ERROR) jt;
SELECT * FROM JSON_TABLE(jsonb '1', '$' COLUMNS (a int PATH 'lax $.a' ERROR ON EMPTY) ERROR ON ERROR) jt;

SELECT * FROM JSON_TABLE(jsonb '"a"', '$' COLUMNS (a int PATH '$'   DEFAULT 1 ON EMPTY DEFAULT 2 ON ERROR)) jt;
SELECT * FROM JSON_TABLE(jsonb '"a"', '$' COLUMNS (a int PATH 'strict $.a' DEFAULT 1 ON EMPTY DEFAULT 2 ON ERROR)) jt;
SELECT * FROM JSON_TABLE(jsonb '"a"', '$' COLUMNS (a int PATH 'lax $.a' DEFAULT 1 ON EMPTY DEFAULT 2 ON ERROR)) jt;

-- JSON_TABLE: EXISTS PATH types
SELECT * FROM JSON_TABLE(jsonb '"a"', '$' COLUMNS (a int4 EXISTS PATH '$.a'));
SELECT * FROM JSON_TABLE(jsonb '"a"', '$' COLUMNS (a int2 EXISTS PATH '$.a'));
SELECT * FROM JSON_TABLE(jsonb '"a"', '$' COLUMNS (a int8 EXISTS PATH '$.a'));
SELECT * FROM JSON_TABLE(jsonb '"a"', '$' COLUMNS (a float4 EXISTS PATH '$.a'));
SELECT * FROM JSON_TABLE(jsonb '"a"', '$' COLUMNS (a char(3) EXISTS PATH '$.a'));
SELECT * FROM JSON_TABLE(jsonb '"a"', '$' COLUMNS (a json EXISTS PATH '$.a'));
SELECT * FROM JSON_TABLE(jsonb '"a"', '$' COLUMNS (a jsonb EXISTS PATH '$.a'));

-- JSON_TABLE: WRAPPER/QUOTES clauses on scalar columns
SELECT * FROM JSON_TABLE(jsonb '"world"', '$' COLUMNS (item text PATH '$' KEEP QUOTES ON SCALAR STRING));
SELECT * FROM JSON_TABLE(jsonb '"world"', '$' COLUMNS (item text PATH '$' OMIT QUOTES ON SCALAR STRING));
SELECT * FROM JSON_TABLE(jsonb '"world"', '$' COLUMNS (item text FORMAT JSON PATH '$' KEEP QUOTES));
SELECT * FROM JSON_TABLE(jsonb '"world"', '$' COLUMNS (item text FORMAT JSON PATH '$' OMIT QUOTES));
SELECT * FROM JSON_TABLE(jsonb '"world"', '$' COLUMNS (item text FORMAT JSON PATH '$' WITHOUT WRAPPER KEEP QUOTES));
SELECT * FROM JSON_TABLE(jsonb '"world"', '$' COLUMNS (item text PATH '$' WITHOUT WRAPPER OMIT QUOTES));

SELECT * FROM JSON_TABLE(jsonb '"world"', '$' COLUMNS (item text FORMAT JSON PATH '$' WITH WRAPPER));

-- Error: QUOTES clause meaningless when WITH WRAPPER is present
SELECT * FROM JSON_TABLE(jsonb '"world"', '$' COLUMNS (item text FORMAT JSON PATH '$' WITH WRAPPER KEEP QUOTES));
SELECT * FROM JSON_TABLE(jsonb '"world"', '$' COLUMNS (item text PATH '$' WITH WRAPPER OMIT QUOTES));

-- JSON_TABLE: nested paths and plans

-- Should fail (JSON_TABLE columns must contain explicit AS path
-- specifications if explicit PLAN clause is used)
SELECT * FROM JSON_TABLE(
	jsonb '[]', '$' -- AS <path name> required here
	COLUMNS (
		foo int PATH '$'
	)
	PLAN DEFAULT (UNION)
) jt;

SELECT * FROM JSON_TABLE(
	jsonb '[]', '$' AS path1
	COLUMNS (
		NESTED PATH '$' COLUMNS ( -- AS <path name> required here
			foo int PATH '$'
		)
	)
	PLAN DEFAULT (UNION)
) jt;

-- Should fail (column names must be distinct)
SELECT * FROM JSON_TABLE(
	jsonb '[]', '$' AS a
	COLUMNS (
		a int
	)
) jt;

SELECT * FROM JSON_TABLE(
	jsonb '[]', '$' AS a
	COLUMNS (
		b int,
		NESTED PATH '$' AS a
		COLUMNS (
			c int
		)
	)
) jt;

SELECT * FROM JSON_TABLE(
	jsonb '[]', '$'
	COLUMNS (
		b int,
		NESTED PATH '$' AS b
		COLUMNS (
			c int
		)
	)
) jt;

SELECT * FROM JSON_TABLE(
	jsonb '[]', '$'
	COLUMNS (
		NESTED PATH '$' AS a
		COLUMNS (
			b int
		),
		NESTED PATH '$'
		COLUMNS (
			NESTED PATH '$' AS a
			COLUMNS (
				c int
			)
		)
	)
) jt;

-- JSON_TABLE: plan validation

SELECT * FROM JSON_TABLE(
	jsonb 'null', '$[*]' AS p0
	COLUMNS (
		NESTED '$' AS p1 COLUMNS (
			NESTED PATH '$' AS p11 COLUMNS ( foo int ),
			NESTED PATH '$' AS p12 COLUMNS ( bar int )
		),
		NESTED PATH '$' AS p2 COLUMNS (
			NESTED PATH '$' AS p21 COLUMNS ( baz int )
		)
	)
	PLAN (p1)
) jt;

SELECT * FROM JSON_TABLE(
	jsonb 'null', '$[*]' AS p0
	COLUMNS (
		NESTED PATH '$' AS p1 COLUMNS (
			NESTED PATH '$' AS p11 COLUMNS ( foo int ),
			NESTED PATH '$' AS p12 COLUMNS ( bar int )
		),
		NESTED PATH '$' AS p2 COLUMNS (
			NESTED PATH '$' AS p21 COLUMNS ( baz int )
		)
	)
	PLAN (p0)
) jt;

SELECT * FROM JSON_TABLE(
	jsonb 'null', '$[*]' AS p0
	COLUMNS (
		NESTED PATH '$' AS p1 COLUMNS (
			NESTED PATH '$' AS p11 COLUMNS ( foo int ),
			NESTED PATH '$' AS p12 COLUMNS ( bar int )
		),
		NESTED PATH '$' AS p2 COLUMNS (
			NESTED PATH '$' AS p21 COLUMNS ( baz int )
		)
	)
	PLAN (p0 OUTER p3)
) jt;

SELECT * FROM JSON_TABLE(
	jsonb 'null', '$[*]' AS p0
	COLUMNS (
		NESTED PATH '$' AS p1 COLUMNS (
			NESTED PATH '$' AS p11 COLUMNS ( foo int ),
			NESTED PATH '$' AS p12 COLUMNS ( bar int )
		),
		NESTED PATH '$' AS p2 COLUMNS (
			NESTED PATH '$' AS p21 COLUMNS ( baz int )
		)
	)
	PLAN (p0 UNION p1 UNION p11)
) jt;

SELECT * FROM JSON_TABLE(
	jsonb 'null', '$[*]' AS p0
	COLUMNS (
		NESTED PATH '$' AS p1 COLUMNS (
			NESTED PATH '$' AS p11 COLUMNS ( foo int ),
			NESTED PATH '$' AS p12 COLUMNS ( bar int )
		),
		NESTED PATH '$' AS p2 COLUMNS (
			NESTED PATH '$' AS p21 COLUMNS ( baz int )
		)
	)
	PLAN (p0 OUTER (p1 CROSS p13))
) jt;

SELECT * FROM JSON_TABLE(
	jsonb 'null', '$[*]' AS p0
	COLUMNS (
		NESTED PATH '$' AS p1 COLUMNS (
			NESTED PATH '$' AS p11 COLUMNS ( foo int ),
			NESTED PATH '$' AS p12 COLUMNS ( bar int )
		),
		NESTED PATH '$' AS p2 COLUMNS (
			NESTED PATH '$' AS p21 COLUMNS ( baz int )
		)
	)
	PLAN (p0 OUTER (p1 CROSS p2))
) jt;

SELECT * FROM JSON_TABLE(
	jsonb 'null', '$[*]' AS p0
	COLUMNS (
		NESTED PATH '$' AS p1 COLUMNS (
			NESTED PATH '$' AS p11 COLUMNS ( foo int ),
			NESTED PATH '$' AS p12 COLUMNS ( bar int )
		),
		NESTED PATH '$' AS p2 COLUMNS (
			NESTED PATH '$' AS p21 COLUMNS ( baz int )
		)
	)
	PLAN (p0 OUTER ((p1 UNION p11) CROSS p2))
) jt;

SELECT * FROM JSON_TABLE(
	jsonb 'null', '$[*]' AS p0
	COLUMNS (
		NESTED PATH '$' AS p1 COLUMNS (
			NESTED PATH '$' AS p11 COLUMNS ( foo int ),
			NESTED PATH '$' AS p12 COLUMNS ( bar int )
		),
		NESTED PATH '$' AS p2 COLUMNS (
			NESTED PATH '$' AS p21 COLUMNS ( baz int )
		)
	)
	PLAN (p0 OUTER ((p1 INNER p11) CROSS p2))
) jt;

SELECT * FROM JSON_TABLE(
	jsonb 'null', '$[*]' AS p0
	COLUMNS (
		NESTED PATH '$' AS p1 COLUMNS (
			NESTED PATH '$' AS p11 COLUMNS ( foo int ),
			NESTED PATH '$' AS p12 COLUMNS ( bar int )
		),
		NESTED PATH '$' AS p2 COLUMNS (
			NESTED PATH '$' AS p21 COLUMNS ( baz int )
		)
	)
	PLAN (p0 OUTER ((p1 INNER (p12 CROSS p11)) CROSS p2))
) jt;

SELECT * FROM JSON_TABLE(
	jsonb 'null', 'strict $[*]' AS p0
	COLUMNS (
		NESTED PATH '$' AS p1 COLUMNS (
			NESTED PATH '$' AS p11 COLUMNS ( foo int ),
			NESTED PATH '$' AS p12 COLUMNS ( bar int )
		),
		NESTED PATH '$' AS p2 COLUMNS (
			NESTED PATH '$' AS p21 COLUMNS ( baz int )
		)
	)
	PLAN (p0 OUTER ((p1 INNER (p12 CROSS p11)) CROSS (p2 INNER p21)))
) jt;

SELECT * FROM JSON_TABLE(
	jsonb 'null', 'strict $[*]' -- without root path name
	COLUMNS (
		NESTED PATH '$' AS p1 COLUMNS (
			NESTED PATH '$' AS p11 COLUMNS ( foo int ),
			NESTED PATH '$' AS p12 COLUMNS ( bar int )
		),
		NESTED PATH '$' AS p2 COLUMNS (
			NESTED PATH '$' AS p21 COLUMNS ( baz int )
		)
	)
	PLAN ((p1 INNER (p12 CROSS p11)) CROSS (p2 INNER p21))
) jt;

-- JSON_TABLE: plan execution

CREATE TEMP TABLE jsonb_table_test (js jsonb);

INSERT INTO jsonb_table_test
VALUES (
	'[
		{"a":  1,  "b": [], "c": []},
		{"a":  2,  "b": [1, 2, 3], "c": [10, null, 20]},
		{"a":  3,  "b": [1, 2], "c": []},
		{"x": "4", "b": [1, 2], "c": 123}
	 ]'
);

-- unspecified plan (outer, union)
select
	jt.*
from
	jsonb_table_test jtt,
	json_table (
		jtt.js,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
	) jt;

-- default plan (outer, union)
select
	jt.*
from
	jsonb_table_test jtt,
	json_table (
		jtt.js,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan default (outer, union)
	) jt;

-- specific plan (p outer (pb union pc))
select
	jt.*
from
	jsonb_table_test jtt,
	json_table (
		jtt.js,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan (p outer (pb union pc))
	) jt;

-- specific plan (p outer (pc union pb))
select
	jt.*
from
	jsonb_table_test jtt,
	json_table (
		jtt.js,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan (p outer (pc union pb))
	) jt;

-- default plan (inner, union)
select
	jt.*
from
	jsonb_table_test jtt,
	json_table (
		jtt.js,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan default (inner)
	) jt;

-- specific plan (p inner (pb union pc))
select
	jt.*
from
	jsonb_table_test jtt,
	json_table (
		jtt.js,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan (p inner (pb union pc))
	) jt;

-- default plan (inner, cross)
select
	jt.*
from
	jsonb_table_test jtt,
	json_table (
		jtt.js,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan default (cross, inner)
	) jt;

-- specific plan (p inner (pb cross pc))
select
	jt.*
from
	jsonb_table_test jtt,
	json_table (
		jtt.js,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan (p inner (pb cross pc))
	) jt;

-- default plan (outer, cross)
select
	jt.*
from
	jsonb_table_test jtt,
	json_table (
		jtt.js,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan default (outer, cross)
	) jt;

-- specific plan (p outer (pb cross pc))
select
	jt.*
from
	jsonb_table_test jtt,
	json_table (
		jtt.js,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan (p outer (pb cross pc))
	) jt;


select
	jt.*, b1 + 100 as b
from
	json_table (jsonb
		'[
			{"a":  1,  "b": [[1, 10], [2], [3, 30, 300]], "c": [1, null, 2]},
			{"a":  2,  "b": [10, 20], "c": [1, null, 2]},
			{"x": "3", "b": [11, 22, 33, 44]}
		 ]',
		'$[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on error,
			nested path 'strict $.b[*]' as pb columns (
				b text format json path '$',
				nested path 'strict $[*]' as pb1 columns (
					b1 int path '$'
				)
			),
			nested path 'strict $.c[*]' as pc columns (
				c text format json path '$',
				nested path 'strict $[*]' as pc1 columns (
					c1 int path '$'
				)
			)
		)
		--plan default(outer, cross)
		plan(p outer ((pb inner pb1) cross (pc outer pc1)))
	) jt;

-- Should succeed (JSON arguments are passed to root and nested paths)
SELECT *
FROM
	generate_series(1, 4) x,
	generate_series(1, 3) y,
	JSON_TABLE(jsonb
		'[[1,2,3],[2,3,4,5],[3,4,5,6]]',
		'strict $[*] ? (@[*] < $x)'
		PASSING x AS x, y AS y
		COLUMNS (
			y text FORMAT JSON PATH '$',
			NESTED PATH 'strict $[*] ? (@ >= $y)'
			COLUMNS (
				z int PATH '$'
			)
		)
	) jt;

-- Should fail (JSON arguments are not passed to column paths)
SELECT *
FROM JSON_TABLE(
	jsonb '[1,2,3]',
	'$[*] ? (@ < $x)'
		PASSING 10 AS x
		COLUMNS (y text FORMAT JSON PATH '$ ? (@ < $x)')
	) jt;

-- Should fail (not supported)
SELECT * FROM JSON_TABLE(jsonb '{"a": 123}', '$' || '.' || 'a' COLUMNS (foo int));
