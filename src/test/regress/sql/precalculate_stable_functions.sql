--
-- PRECALCULATE STABLE FUNCTIONS
--
-- Create types and tables for testing

CREATE TYPE my_integer AS (value integer);
CREATE TYPE composite_type AS (first integer, second integer[], third boolean);

CREATE TABLE x (x integer);
INSERT INTO x SELECT generate_series(1, 4) x;

CREATE TABLE wxyz (w integer, x integer[], y boolean, z integer);
CREATE TABLE wxyz_child () INHERITS (wxyz);
CREATE TABLE wxyz_child2 (a integer, b integer) INHERITS (wxyz);

CREATE TABLE no_columns ();
CREATE TABLE no_columns_child () INHERITS (no_columns);
CREATE TABLE no_columns_child2 (a integer, b integer) INHERITS (no_columns);

-- Create volatile functions for testing

CREATE OR REPLACE FUNCTION public.x_vlt (
)
RETURNS integer VOLATILE AS
$body$
BEGIN
  RAISE NOTICE 'v';
  RETURN 1;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_vlt_my_integer (
)
RETURNS my_integer VOLATILE AS
$body$
BEGIN
  RAISE NOTICE 'v my_integer';
  RETURN '(1)'::my_integer;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_vlt_array_integer (
)
RETURNS int[] VOLATILE AS
$body$
BEGIN
  RAISE NOTICE 'v array_integer';
  RETURN '{2, 3}'::integer[];
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_vlt_boolean (
)
RETURNS boolean VOLATILE AS
$body$
BEGIN
  RAISE NOTICE 'v boolean';
  RETURN TRUE;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_vlt_wxyz (
)
RETURNS wxyz VOLATILE AS
$body$
BEGIN
  RAISE NOTICE 'v wxyz';
  RETURN '(1, {2}, TRUE, 3)'::wxyz;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_vlt_wxyz_child (
)
RETURNS wxyz_child VOLATILE AS
$body$
BEGIN
  RAISE NOTICE 'v wxyz_child';
  RETURN '(1, {2}, TRUE, 3)'::wxyz_child;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_vlt_wxyz_child2 (
)
RETURNS wxyz_child2 VOLATILE AS
$body$
BEGIN
  RAISE NOTICE 'v wxyz_child2';
  RETURN '(1, {2}, TRUE, 3, 4, 5)'::wxyz_child2;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_vlt_oid (
)
RETURNS oid VOLATILE AS
$body$
BEGIN
  RAISE NOTICE 'v oid';
  RETURN 1;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_vlt_text_integer (
)
RETURNS text VOLATILE AS
$body$
BEGIN
  RAISE NOTICE 'v text integer';
  RETURN 1::text;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_vlt_text_my_integer (
)
RETURNS text VOLATILE AS
$body$
BEGIN
  RAISE NOTICE 'v text my_integer';
  RETURN '(1)'::text;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_vlt_text_xml (
)
RETURNS text VOLATILE AS
$body$
BEGIN
  RAISE NOTICE 'v text xml';
  RETURN '<?xml version="1.0"?><book><title>Manual</title></book>'::text;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_vlt_text_xml_content (
)
RETURNS text VOLATILE AS
$body$
BEGIN
  RAISE NOTICE 'v text xml content';
  RETURN 'abc<foo>bar</foo><bar>foo</bar>'::text;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_vlt_text_xml_instruction_content (
)
RETURNS text VOLATILE AS
$body$
BEGIN
  RAISE NOTICE 'v text xml instruction content';
  RETURN 'echo "hello world";'::text;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_vlt_xml (
)
RETURNS xml VOLATILE AS
$body$
BEGIN
  RAISE NOTICE 'v xml';
  RETURN '<bar>foo</bar>'::xml;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_vlt_xml_content (
)
RETURNS xml VOLATILE AS
$body$
BEGIN
  RAISE NOTICE 'v xml content';
  RETURN 'abc<foo>bar</foo><bar>foo</bar>'::xml;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_vlt2 (
  integer
)
RETURNS integer VOLATILE AS
$body$
BEGIN
  RAISE NOTICE 'v2';
  RETURN $1;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.equal_integers_vlt (
  integer,
  integer
)
RETURNS boolean VOLATILE AS
$body$
BEGIN
  RAISE NOTICE 'equal integers volatile';
  RETURN $1 = $2;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.equal_my_integer_vlt (
  my_integer,
  my_integer
)
RETURNS boolean VOLATILE AS
$body$
BEGIN
  RAISE NOTICE 'equal my_integer volatile';
  RETURN $1.value = $2.value;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.cast_integer_as_my_integer_vlt (
  integer
)
RETURNS my_integer VOLATILE AS
$body$
BEGIN
  RAISE NOTICE 'cast integer as my_integer volatile';
  RETURN ROW($1)::my_integer;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.cast_my_integer_as_integer_vlt (
  my_integer
)
RETURNS integer VOLATILE AS
$body$
BEGIN
  RAISE NOTICE 'cast my_integer as integer volatile';
  RETURN $1.value;
END;
$body$
LANGUAGE 'plpgsql';

-- Create stable functions for testing

CREATE OR REPLACE FUNCTION public.x_stl (
)
RETURNS integer STABLE AS
$body$
BEGIN
  RAISE NOTICE 's';
  RETURN 1;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl_my_integer (
)
RETURNS my_integer STABLE AS
$body$
BEGIN
  RAISE NOTICE 's my_integer';
  RETURN '(1)'::my_integer;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl_array_integer (
)
RETURNS int[] STABLE AS
$body$
BEGIN
  RAISE NOTICE 's array_integer';
  RETURN '{2, 3}'::integer[];
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl_wxyz (
)
RETURNS wxyz STABLE AS
$body$
BEGIN
  RAISE NOTICE 's wxyz';
  RETURN '(1, {2}, TRUE, 3)'::wxyz;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl_wxyz_child (
)
RETURNS wxyz_child STABLE AS
$body$
BEGIN
  RAISE NOTICE 's wxyz_child';
  RETURN '(1, {2}, TRUE, 3)'::wxyz_child;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl_wxyz_child2 (
)
RETURNS wxyz_child2 STABLE AS
$body$
BEGIN
  RAISE NOTICE 's wxyz_child2';
  RETURN '(1, {2}, TRUE, 3, 4, 5)'::wxyz_child2;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl_oid (
)
RETURNS oid STABLE AS
$body$
BEGIN
  RAISE NOTICE 's oid';
  RETURN 1;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl_text_integer (
)
RETURNS text STABLE AS
$body$
BEGIN
  RAISE NOTICE 's text integer';
  RETURN 1::text;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl_text_my_integer (
)
RETURNS text STABLE AS
$body$
BEGIN
  RAISE NOTICE 's text my_integer';
  RETURN '(1)'::text;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl_text_xml (
)
RETURNS text STABLE AS
$body$
BEGIN
  RAISE NOTICE 's text xml';
  RETURN '<?xml version="1.0"?><book><title>Manual</title></book>'::text;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl_text_xml_content (
)
RETURNS text STABLE AS
$body$
BEGIN
  RAISE NOTICE 's xml content';
  RETURN 'abc<foo>bar</foo><bar>foo</bar>'::text;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl_text_xml_instruction_content (
)
RETURNS text STABLE AS
$body$
BEGIN
  RAISE NOTICE 's text xml instruction content';
  RETURN 'echo "hello world";'::text;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl_xml (
)
RETURNS xml STABLE AS
$body$
BEGIN
  RAISE NOTICE 's xml';
  RETURN '<bar>foo</bar>'::xml;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl_xml_content (
)
RETURNS xml STABLE AS
$body$
BEGIN
  RAISE NOTICE 's xml content';
  RETURN 'abc<foo>bar</foo><bar>foo</bar>'::xml;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl2 (
  integer
)
RETURNS integer STABLE AS
$body$
BEGIN
  RAISE NOTICE 's2';
  RETURN $1;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl2_strict (
  integer
)
RETURNS integer STABLE STRICT AS
$body$
BEGIN
  RAISE NOTICE 's2 strict';
  RETURN $1;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl2_boolean (
  boolean
)
RETURNS boolean STABLE AS
$body$
BEGIN
  RAISE NOTICE 's2 boolean';
  RETURN $1;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl2_array_integer (
  integer[]
)
RETURNS integer[] STABLE AS
$body$
BEGIN
  RAISE NOTICE 's2 array_integer';
  RETURN $1;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl2_array_oid (
  oid[]
)
RETURNS oid[] STABLE AS
$body$
BEGIN
  RAISE NOTICE 's2 array_oid';
  RETURN $1;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl2_wxyz (
  wxyz
)
RETURNS wxyz STABLE AS
$body$
BEGIN
  RAISE NOTICE 's2 wxyz';
  RETURN $1;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl2_composite_type (
  composite_type
)
RETURNS composite_type STABLE AS
$body$
BEGIN
  RAISE NOTICE 's2 composite_type';
  RETURN $1;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl2_my_integer (
  my_integer
)
RETURNS my_integer STABLE AS
$body$
BEGIN
  RAISE NOTICE 's2 my_integer';
  RETURN $1;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl2_no_columns (
  no_columns
)
RETURNS no_columns STABLE AS
$body$
BEGIN
  RAISE NOTICE 's2 no_columns';
  RETURN $1;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl2_name (
  name
)
RETURNS name STABLE AS
$body$
BEGIN
  RAISE NOTICE 's2 name';
  RETURN $1;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl2_xml (
  xml
)
RETURNS xml STABLE AS
$body$
BEGIN
  RAISE NOTICE 's2 xml';
  RETURN $1;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_stl2_text (
  text
)
RETURNS text STABLE AS
$body$
BEGIN
  RAISE NOTICE 's2 text';
  RETURN $1;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.equal_integers_stl (
  integer,
  integer
)
RETURNS boolean STABLE AS
$body$
BEGIN
  RAISE NOTICE 'equal integers stable';
  RETURN $1 = $2;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.equal_booleans_stl_strict (
  boolean,
  boolean
)
RETURNS boolean STABLE STRICT AS
$body$
BEGIN
  RAISE NOTICE 'equal booleans stable strict';
  RETURN $1 = $2;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.equal_my_integer_stl (
  my_integer,
  my_integer
)
RETURNS boolean STABLE AS
$body$
BEGIN
  RAISE NOTICE 'equal my_integer stable';
  RETURN $1.value = $2.value;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.cast_integer_as_my_integer_stl (
  integer
)
RETURNS my_integer STABLE AS
$body$
BEGIN
  RAISE NOTICE 'cast integer as my_integer stable';
  RETURN ROW($1)::my_integer;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.cast_my_integer_as_integer_stl (
  my_integer
)
RETURNS integer STABLE AS
$body$
BEGIN
  RAISE NOTICE 'cast my_integer as integer stable';
  RETURN $1.value;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.stable_max(
)
RETURNS integer STABLE AS
$body$
BEGIN
  RETURN (SELECT max(x) from x);
END
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.simple(
)
RETURNS integer STABLE AS
$body$
BEGIN
  RETURN stable_max();
END
$body$
LANGUAGE 'plpgsql';

-- Create immutable functions for testing

CREATE OR REPLACE FUNCTION public.x_imm2 (
  integer
)
RETURNS integer IMMUTABLE AS
$body$
BEGIN
  RAISE NOTICE 'i2';
  RETURN $1;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_imm2_strict (
  integer
)
RETURNS integer IMMUTABLE STRICT AS
$body$
BEGIN
  RAISE NOTICE 'i2 strict';
  RETURN $1;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.x_imm2_my_integer (
  my_integer
)
RETURNS my_integer IMMUTABLE AS
$body$
BEGIN
  RAISE NOTICE 'i2 my_integer';
  RETURN $1;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.equal_integers_imm (
  integer,
  integer
)
RETURNS boolean IMMUTABLE AS
$body$
BEGIN
  RAISE NOTICE 'equal integers immutable';
  RETURN $1 = $2;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.equal_booleans_imm (
  boolean,
  boolean
)
RETURNS boolean IMMUTABLE AS
$body$
BEGIN
  RAISE NOTICE 'equal booleans immutable';
  RETURN $1 = $2;
END;
$body$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION public.equal_my_integer_imm (
  my_integer,
  my_integer
)
RETURNS boolean IMMUTABLE AS
$body$
BEGIN
  RAISE NOTICE 'equal my_integer immutable';
  RETURN $1.value = $2.value;
END;
$body$
LANGUAGE 'plpgsql';

-- Create operators for testing

CREATE OPERATOR === (
  PROCEDURE = equal_integers_vlt,
  LEFTARG = integer,
  RIGHTARG = integer
);

CREATE OPERATOR ==== (
  PROCEDURE = equal_integers_stl,
  LEFTARG = integer,
  RIGHTARG = integer
);

CREATE OPERATOR ===== (
  PROCEDURE = equal_integers_imm,
  LEFTARG = integer,
  RIGHTARG = integer
);

CREATE OPERATOR ==== (
  PROCEDURE = equal_booleans_stl_strict,
  LEFTARG = boolean,
  RIGHTARG = boolean
);

CREATE OPERATOR ===== (
  PROCEDURE = equal_booleans_imm,
  LEFTARG = boolean,
  RIGHTARG = boolean
);

-- Functions testing

-- Simple functions testing
SELECT x_vlt() FROM x; -- should not be precalculated
SELECT x_stl() FROM x;

-- WHERE clause testing
SELECT x_vlt() FROM x WHERE x_vlt() < x; -- should not be precalculated
SELECT x_stl() FROM x WHERE x_stl() < x;

-- JOIN/ON clause testing

-- should not be precalculated
SELECT * FROM x JOIN generate_series(1, 2) y ON x_vlt() < x;

SELECT * FROM x JOIN generate_series(1, 2) y ON x_stl() < x;

-- Functions with constant arguments testing
SELECT x_vlt2(1) FROM x; -- should not be precalculated
SELECT x_stl2(1) FROM x;

-- Nested functions testing
SELECT x_stl2(x_vlt()) FROM x; -- should not be precalculated
SELECT x_imm2(x_vlt()) FROM x; -- should not be precalculated

SELECT x_stl2(x_stl()) FROM x;
SELECT x_imm2(x_stl()) FROM x;

-- Strict functions testing
SELECT x_stl2_strict(x_vlt()) FROM x; -- should not be precalculated
SELECT x_imm2_strict(x_vlt()) FROM x; -- should not be precalculated

SELECT x_stl2_strict(x_stl2_strict(1)) FROM x;
SELECT x_imm2_strict(x_stl2_strict(1)) FROM x;

-- Strict functions with null arguments testing
SELECT x_stl2_strict(x_stl2(NULL)) FROM x;
SELECT x_imm2_strict(x_stl2(NULL)) FROM x;

-- Operators testing

SELECT 1 === 2 FROM x; -- should not be precalculated
SELECT 1 ==== 2 FROM x;

-- Strict operators testing
SELECT x_stl2_boolean(NULL) ==== TRUE FROM x;
SELECT x_stl2_boolean(NULL) ===== TRUE FROM x;

-- Mixed functions and operators testing
SELECT x_stl2_boolean(1 === 2) FROM x; -- should not be precalculated
SELECT x_stl2_boolean(1 ==== 2) FROM x;

SELECT x_vlt() ==== 1 FROM x; -- should not be precalculated
SELECT x_stl() ==== 1 FROM x;

-- IS (NOT) DISTINCT FROM expression testing

-- create operator here because we will drop and reuse it several times
CREATE OPERATOR = (
  PROCEDURE = equal_my_integer_vlt,
  LEFTARG = my_integer,
  RIGHTARG = my_integer
);

-- should not be precalculated
SELECT '(1)'::my_integer IS DISTINCT FROM '(2)'::my_integer FROM x;

-- should not be precalculated
SELECT '(1)'::my_integer IS NOT DISTINCT FROM '(2)'::my_integer FROM x;

DROP OPERATOR = (my_integer, my_integer);
CREATE OPERATOR = (
  PROCEDURE = equal_my_integer_stl,
  LEFTARG = my_integer,
  RIGHTARG = my_integer
);

SELECT '(1)'::my_integer IS DISTINCT FROM '(2)'::my_integer FROM x;
SELECT '(1)'::my_integer IS NOT DISTINCT FROM '(2)'::my_integer FROM x;

-- IS (NOT) DISTINCT FROM expressions with null arguments testing
SELECT x_stl2_boolean(x_stl2(NULL) IS DISTINCT FROM 1) FROM x;
SELECT x_stl2_boolean(x_stl2(NULL) IS NOT DISTINCT FROM 1) FROM x;

SELECT x_stl2_boolean(x_stl2(NULL) IS DISTINCT FROM x_stl2(NULL)) FROM x;
SELECT x_stl2_boolean(x_stl2(NULL) IS NOT DISTINCT FROM x_stl2(NULL)) FROM x;

-- Mixed functions and IS (NOT) DISTINCT FROM expressions testing
DROP OPERATOR = (my_integer, my_integer);
CREATE OPERATOR = (
  PROCEDURE = equal_my_integer_vlt,
  LEFTARG = my_integer,
  RIGHTARG = my_integer
);

-- should not be precalculated
SELECT equal_booleans_stl_strict(
  ('(1)'::my_integer IS DISTINCT FROM '(1)'::my_integer),
  ('(1)'::my_integer IS NOT DISTINCT FROM '(1)'::my_integer)
)
FROM x;

DROP OPERATOR = (my_integer, my_integer);
CREATE OPERATOR = (
  PROCEDURE = equal_my_integer_stl,
  LEFTARG = my_integer,
  RIGHTARG = my_integer
);

SELECT equal_booleans_stl_strict(
  ('(1)'::my_integer IS DISTINCT FROM '(1)'::my_integer),
  ('(1)'::my_integer IS NOT DISTINCT FROM '(1)'::my_integer)
)
FROM x;

-- should not be precalculated
SELECT (x_vlt_my_integer() IS DISTINCT FROM '(1)'::my_integer) FROM x;

-- should not be precalculated
SELECT (x_vlt_my_integer() IS NOT DISTINCT FROM '(1)'::my_integer) FROM x;

SELECT (x_stl_my_integer() IS DISTINCT FROM '(1)'::my_integer) FROM x;
SELECT (x_stl_my_integer() IS NOT DISTINCT FROM '(1)'::my_integer) FROM x;

-- NULLIF expressions testing

DROP OPERATOR = (my_integer, my_integer);
CREATE OPERATOR = (
  PROCEDURE = equal_my_integer_vlt,
  LEFTARG = my_integer,
  RIGHTARG = my_integer
);

-- should not be precalculated
SELECT NULLIF('(1)'::my_integer, '(2)'::my_integer) FROM x;

DROP OPERATOR = (my_integer, my_integer);
CREATE OPERATOR = (
  PROCEDURE = equal_my_integer_stl,
  LEFTARG = my_integer,
  RIGHTARG = my_integer
);

SELECT NULLIF('(1)'::my_integer, '(2)'::my_integer) FROM x;

DROP OPERATOR = (my_integer, my_integer);
CREATE OPERATOR = (
  PROCEDURE = equal_my_integer_imm,
  LEFTARG = my_integer,
  RIGHTARG = my_integer
);

SELECT NULLIF('(1)'::my_integer, '(2)'::my_integer) FROM x;

-- NULLIF expressions with null arguments testing
SELECT x_stl2(NULLIF(1, NULL)) FROM x;
SELECT x_stl2(NULLIF(NULL::integer, NULL)) FROM x;

-- Mixed functions and NULLIF expressions testing
DROP OPERATOR = (my_integer, my_integer);
CREATE OPERATOR = (
  PROCEDURE = equal_my_integer_vlt,
  LEFTARG = my_integer,
  RIGHTARG = my_integer
);

-- should not be precalculated
SELECT equal_my_integer_stl(
  NULLIF('(1)'::my_integer, '(2)'::my_integer),
  NULLIF('(2)'::my_integer, '(2)'::my_integer)
)
FROM x;

DROP OPERATOR = (my_integer, my_integer);
CREATE OPERATOR = (
  PROCEDURE = equal_my_integer_stl,
  LEFTARG = my_integer,
  RIGHTARG = my_integer
);

SELECT equal_my_integer_stl(
  NULLIF('(1)'::my_integer, '(2)'::my_integer),
  NULLIF('(2)'::my_integer, '(2)'::my_integer)
)
FROM x;

-- should not be precalculated
SELECT NULLIF(x_vlt_my_integer(), '(2)'::my_integer) FROM x;

SELECT NULLIF(x_stl_my_integer(), '(2)'::my_integer) FROM x;

-- "scalar op ANY/ALL (array)" / "scalar IN (2 or more values)" expressions
-- testing

SELECT 1 === ANY ('{2, 3}') FROM x; -- should not be precalculated
SELECT 1 === ALL ('{2, 3}') FROM x; -- should not be precalculated

DROP OPERATOR = (my_integer, my_integer);
CREATE OPERATOR = (
  PROCEDURE = equal_my_integer_vlt,
  LEFTARG = my_integer,
  RIGHTARG = my_integer
);

-- should not be precalculated
SELECT '(1)'::my_integer IN ('(2)'::my_integer, '(3)'::my_integer) FROM x;

SELECT 1 ==== ANY ('{2, 3}') FROM x;
SELECT 1 ==== ALL ('{2, 3}') FROM x;

DROP OPERATOR = (my_integer, my_integer);
CREATE OPERATOR = (
  PROCEDURE = equal_my_integer_stl,
  LEFTARG = my_integer,
  RIGHTARG = my_integer
);

SELECT '(1)'::my_integer IN ('(2)'::my_integer, '(3)'::my_integer) FROM x;

SELECT 1 ===== ANY ('{2, 3}') FROM x;
SELECT 1 ===== ALL ('{2, 3}') FROM x;

DROP OPERATOR = (my_integer, my_integer);
CREATE OPERATOR = (
  PROCEDURE = equal_my_integer_imm,
  LEFTARG = my_integer,
  RIGHTARG = my_integer
);

SELECT '(1)'::my_integer IN ('(2)'::my_integer, '(3)'::my_integer) FROM x;

-- "scalar op ANY/ALL (array)" / "scalar IN (2 or more values)" expressions with
-- null arguments testing
SELECT 1 ==== ANY ('{2, NULL}') FROM x;
SELECT x_stl2_boolean(1 ==== ANY (NULL)) FROM x;
SELECT NULL ==== ANY ('{2, 3}'::integer[]) FROM x;
SELECT NULL ==== ANY ('{2, NULL}'::integer[]) FROM x;
SELECT x_stl2_boolean(NULL::integer ==== ANY (NULL)) FROM x;

SELECT 1 ==== ALL ('{2, NULL}') FROM x;
SELECT x_stl2_boolean(1 ==== ALL (NULL)) FROM x;
SELECT NULL ==== ALL ('{2, 3}'::integer[]) FROM x;
SELECT NULL ==== ALL ('{2, NULL}'::integer[]) FROM x;
SELECT x_stl2_boolean(NULL::integer ==== ALL (NULL)) FROM x;

SELECT x_stl2_boolean(1 IN (2, NULL)) FROM x;
SELECT x_stl2_boolean(NULL IN (2, 3)) FROM x;
SELECT x_stl2_boolean(NULL IN (2, NULL)) FROM x;

-- Mixed functions and "scalar op ANY/ALL (array)" / "scalar IN (2 or more
-- values)" expressions testing

-- should not be precalculated
SELECT x_stl2_boolean(1 === ANY ('{2, 3}')) FROM x;

-- should not be precalculated
SELECT x_stl2_boolean(1 === ALL ('{2, 3}')) FROM x;

DROP OPERATOR = (my_integer, my_integer);
CREATE OPERATOR = (
  PROCEDURE = equal_my_integer_vlt,
  LEFTARG = my_integer,
  RIGHTARG = my_integer
);

-- should not be precalculated
SELECT x_stl2_boolean(
  '(1)'::my_integer IN ('(2)'::my_integer, '(3)'::my_integer)
)
FROM x;

SELECT x_stl2_boolean(1 ==== ANY ('{2, 3}')) FROM x;
SELECT x_stl2_boolean(1 ==== ALL ('{2, 3}')) FROM x;

DROP OPERATOR = (my_integer, my_integer);
CREATE OPERATOR = (
  PROCEDURE = equal_my_integer_stl,
  LEFTARG = my_integer,
  RIGHTARG = my_integer
);

SELECT x_stl2_boolean(
  '(1)'::my_integer IN ('(2)'::my_integer, '(3)'::my_integer)
)
FROM x;

-- should not be precalculated
SELECT x_vlt() ==== ANY ('{2, 3}') FROM x;

-- should not be precalculated
SELECT x_vlt() ==== ALL ('{2, 3}') FROM x;

-- should not be precalculated
SELECT 1 ==== ANY (x_vlt_array_integer()) FROM x;

-- should not be precalculated
SELECT 1 ==== ALL (x_vlt_array_integer()) FROM x;

-- should not be precalculated
SELECT x_vlt_my_integer() IN ('(2)'::my_integer, '(3)'::my_integer) FROM x;

SELECT x_stl() ==== ANY ('{2, 3}') FROM x;
SELECT x_stl() ==== ALL ('{2, 3}') FROM x;

SELECT 1 ==== ANY (x_stl_array_integer()) FROM x;
SELECT 1 ==== ALL (x_stl_array_integer()) FROM x;

SELECT x_stl_my_integer() IN ('(2)'::my_integer, '(3)'::my_integer) FROM x;

-- Boolean expressions testing

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt_boolean() AND x_stl2_boolean(TRUE)) FROM x;

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt_boolean() OR x_stl2_boolean(TRUE)) FROM x;

-- should not be precalculated
SELECT x_stl2_boolean(NOT x_vlt_boolean()) FROM x;

SELECT x_stl2_boolean(x_stl2_boolean(TRUE) AND x_stl2_boolean(TRUE)) FROM x;
SELECT x_stl2_boolean(x_stl2_boolean(TRUE) OR x_stl2_boolean(TRUE)) FROM x;
SELECT x_stl2_boolean(NOT x_stl2_boolean(TRUE)) FROM x;

-- ARRAY[] expressions testing

-- should not be precalculated
SELECT x_stl2_array_integer(ARRAY[x_vlt(), 2]) FROM x;

SELECT x_stl2_array_integer(ARRAY[x_stl(), 2]) FROM x;

-- Multidimensional ARRAY[] expressions testing

-- should not be precalculated
SELECT x_stl2_array_integer(ARRAY[[x_vlt(), 2], [3, 4]]) FROM x;

SELECT x_stl2_array_integer(ARRAY[[x_stl(), 2], [3, 4]]) FROM x;

-- Array subscripting operations testing

SELECT x_stl2(('{1, 2}'::integer[])[1]) FROM x;
SELECT x_stl2_array_integer(('{1, 2}'::integer[])[:]) FROM x;

-- Mixed functions and array subscripting operations testing

-- should not be precalculated
SELECT x_stl2((x_vlt_array_integer())[x_vlt()]) FROM x;

-- should not be precalculated
SELECT x_stl2((x_vlt_array_integer())[1]) FROM x;

-- should not be precalculated
SELECT x_stl2_array_integer((x_vlt_array_integer())[:]) FROM x;

-- should not be precalculated
SELECT x_stl2(('{1, 2}'::integer[])[x_vlt()]) FROM x;

SELECT x_stl2((x_stl_array_integer())[x_stl()]) FROM x;
SELECT x_stl2((x_stl_array_integer())[1]) FROM x;
SELECT x_stl2_array_integer((x_stl_array_integer())[:]) FROM x;
SELECT x_stl2(('{1, 2}'::integer[])[x_stl()]) FROM x;

-- FieldSelect expressions testing

SELECT x_stl2(('(1, {2}, TRUE, 3)'::wxyz).w) FROM x;
SELECT x_stl2(('(1)'::my_integer).value) FROM x;

-- Mixed functions and FieldSelect expressions testing
SELECT x_stl2((x_vlt_wxyz()).w) FROM x; -- should not be precalculated
SELECT x_stl2((x_vlt_my_integer()).value) FROM x; -- should not be precalculated

SELECT x_stl2((x_stl_wxyz()).w) FROM x;
SELECT x_stl2((x_stl_my_integer()).value) FROM x;

-- ROW() expressions testing

SELECT x_stl2_wxyz((1, '{2}', TRUE, 3)) FROM x;
SELECT x_stl2_wxyz(ROW(1, '{2}', TRUE, 3)) FROM x;
SELECT x_stl2_wxyz((1, '{2}', TRUE, 3)::wxyz) FROM x;

SELECT x_stl2_composite_type((1, '{2}', TRUE)) FROM x;
SELECT x_stl2_composite_type(ROW(1, '{2}', TRUE)) FROM x;
SELECT x_stl2_composite_type((1, '{2}', TRUE)::composite_type) FROM x;

-- Mixed functions and ROW() expressions testing

-- should not be precalculated
SELECT x_stl2_wxyz((x_vlt(), '{2}', TRUE, 3)) FROM x;

SELECT x_stl2_wxyz((x_stl(), '{2}', TRUE, 3)) FROM x;

-- RelabelType expressions testing

-- should not be precalculated
SELECT x_stl2(x_vlt_oid()::integer) FROM x;

SELECT x_stl2(x_stl_oid()::integer) FROM x;

-- CoerceViaIO expressions testing

SELECT x_stl2_my_integer('(1)'::text::my_integer) FROM x;

-- should not be precalculated
SELECT x_stl2(x_vlt_text_integer()::integer) FROM x;

SELECT x_stl2(x_stl_text_integer()::integer) FROM x;

-- Mixed functions and CoerceViaIO expressions testing

-- should not be precalculated
SELECT x_stl2_my_integer(x_vlt_text_my_integer()::my_integer) FROM x;

SELECT x_stl2_my_integer(x_stl_text_my_integer()::my_integer) FROM x;

-- ArrayCoerce expressions testing

-- Binary-coercible types:

-- should not be precalculated
SELECT x_stl2_array_oid(x_vlt_array_integer()::oid[]) FROM x;

SELECT x_stl2_array_oid(x_stl_array_integer()::oid[]) FROM x;

-- Not binary-coercible types:
-- create cast here because we will drop and reuse it several times
CREATE CAST (integer AS my_integer)
  WITH FUNCTION cast_integer_as_my_integer_vlt;

SELECT '{1, 2}'::integer[]::my_integer[] FROM x; -- should not be precalculated

DROP CAST (integer AS my_integer);
CREATE CAST (integer AS my_integer)
  WITH FUNCTION cast_integer_as_my_integer_stl;

SELECT '{1, 2}'::integer[]::my_integer[] FROM x;

-- Mixed functions and ArrayCoerce expressions testing
-- Not binary-coercible types:
-- create cast here because we will drop and reuse it several times
CREATE CAST (my_integer AS integer)
  WITH FUNCTION cast_my_integer_as_integer_vlt;

-- should not be precalculated
SELECT x_stl2_array_integer('{(1), (2)}'::my_integer[]::integer[]) FROM x;

DROP CAST (my_integer AS integer);
CREATE CAST (my_integer AS integer)
  WITH FUNCTION cast_my_integer_as_integer_stl;

SELECT x_stl2_array_integer('{(1), (2)}'::my_integer[]::integer[]) FROM x;

DROP CAST (integer AS my_integer);
CREATE CAST (integer AS my_integer)
  WITH FUNCTION cast_integer_as_my_integer_stl;

-- should not be precalculated
SELECT x_vlt_array_integer()::my_integer[] FROM x;

SELECT x_stl_array_integer()::my_integer[] FROM x;

-- ConvertRowtypeExpr testing

SELECT x_stl2_wxyz('(1, {2}, TRUE, 3)'::wxyz_child::wxyz) FROM x;
SELECT x_stl2_wxyz('(1, {2}, TRUE, 3, 4, 5)'::wxyz_child2::wxyz) FROM x;

SELECT x_stl2_no_columns('()'::no_columns_child::no_columns) FROM x;
SELECT x_stl2_no_columns('(1, 2)'::no_columns_child2::no_columns) FROM x;

-- Mixed functions and ConvertRowtypeExpr testing

-- should not be precalculated
SELECT x_stl2_wxyz(x_vlt_wxyz_child()::wxyz_child::wxyz) FROM x;

-- should not be precalculated
SELECT x_stl2_wxyz(x_vlt_wxyz_child2()::wxyz_child2::wxyz) FROM x;

SELECT x_stl2_wxyz(x_stl_wxyz_child()::wxyz_child::wxyz) FROM x;
SELECT x_stl2_wxyz(x_stl_wxyz_child2()::wxyz_child2::wxyz) FROM x;

-- CASE expressions testing

-- should not be precalculated
SELECT x_stl2(CASE WHEN x_vlt_boolean() THEN x_vlt() ELSE x_vlt() END) FROM x;

-- should not be precalculated
SELECT x_stl2(CASE x_vlt() WHEN x_vlt() THEN x_vlt() ELSE x_vlt() END) FROM x;

SELECT x_stl2(CASE WHEN x_stl2_boolean(TRUE) THEN x_stl() ELSE x_stl() END)
FROM x;

SELECT x_stl2(CASE x_stl() WHEN x_stl() THEN x_stl() ELSE x_stl() END) FROM x;

-- RowCompareExpr testing

SELECT x_stl2_boolean((1, 2) < (1, 3)) FROM x;

-- Mixed functions and RowCompareExpr testing

-- should not be precalculated
SELECT x_stl2_boolean((x_vlt(), 2) < (1, 3)) FROM x;

SELECT x_stl2_boolean((x_stl(), 2) < (1, 3)) FROM x;

-- COALESCE expressions testing

-- should not be precalculated
SELECT x_stl2(COALESCE(NULL, x_vlt2(NULL), 2)) FROM x;

SELECT x_stl2(COALESCE(NULL, x_stl2(NULL), 2)) FROM x;

-- GREATEST and LEAST functions testing

SELECT x_stl2(GREATEST(2, 1, 3)) FROM x;
SELECT x_stl2(LEAST(2, 1, 3)) FROM x;

-- Mixed functions and GREATEST and LEAST functions testing

-- should not be precalculated
SELECT x_stl2(GREATEST(2, x_vlt(), 3)) FROM x;

-- should not be precalculated
SELECT x_stl2(LEAST(2, x_vlt(), 3)) FROM x;

SELECT x_stl2(GREATEST(2, x_stl(), 3)) FROM x;
SELECT x_stl2(LEAST(2, x_stl(), 3)) FROM x;

-- SQLValueFunction testing

CREATE ROLE regress_testrol2 SUPERUSER;
CREATE ROLE regress_testrol1 SUPERUSER LOGIN IN ROLE regress_testrol2;

\c -
SET SESSION AUTHORIZATION regress_testrol1;
SET ROLE regress_testrol2;

SELECT x_stl2_boolean(date(now()) = current_date) FROM x;

SELECT x_stl2_boolean(now()::timetz = current_time) FROM x;
SELECT x_stl2_boolean(now()::timetz(2) = current_time(2)) FROM x; -- precision

SELECT x_stl2_boolean(now() = current_timestamp) FROM x;

-- precision
SELECT x_stl2_boolean(
  length(current_timestamp::text) >= length(current_timestamp(0)::text)
)
FROM x;

SELECT x_stl2_boolean(now()::time = localtime) FROM x;
SELECT x_stl2_boolean(now()::time(2) = localtime(2)) FROM x; -- precision

SELECT x_stl2_boolean(now()::timestamp = localtimestamp) FROM x;

-- precision
SELECT x_stl2_boolean(now()::timestamp(2) = localtimestamp(2)) FROM x;

SELECT x_stl2_name(current_role) FROM x;
SELECT x_stl2_name(current_user) FROM x;
SELECT x_stl2_name(user) FROM x;
SELECT x_stl2_name(session_user) FROM x;
SELECT x_stl2_name(current_catalog) FROM x;
SELECT x_stl2_name(current_schema) FROM x;

\c
DROP ROLE regress_testrol1, regress_testrol2;

-- Xml expressions testing

SELECT x_stl2_xml(XMLCONCAT('<abc/>', '<bar>foo</bar>')) FROM x;

SELECT x_stl2_xml(
  XMLELEMENT(name foo, xmlattributes('bar' as bar), 'cont', 'ent')
)
FROM x;

SELECT x_stl2_xml(XMLFOREST('abc' AS foo, 123 AS bar)) FROM x;

SELECT x_stl2_xml(XMLPARSE(
  DOCUMENT '<?xml version="1.0"?><book><title>Manual</title></book>'
))
FROM x;

SELECT x_stl2_xml(XMLPARSE(CONTENT 'abc<foo>bar</foo><bar>foo</bar>')) FROM x;

SELECT x_stl2_xml(XMLPI(name php, 'echo "hello world";')) FROM x;

SELECT x_stl2_xml(XMLROOT(
  '<?xml version="1.0"?><content>abc</content>',
  version '1.0',
  standalone yes
))
FROM x;

SELECT x_stl2_text(XMLSERIALIZE(
  DOCUMENT '<?xml version="1.0"?><book><title>Manual</title></book>' AS text
))
FROM x;

SELECT x_stl2_text(XMLSERIALIZE(
  CONTENT 'abc<foo>bar</foo><bar>foo</bar>' AS text
))
FROM x;

SELECT x_stl2_boolean('abc<foo>bar</foo><bar>foo</bar>' IS DOCUMENT) FROM x;

-- Mixed functions and Xml expressions testing
-- should not be precalculated
SELECT x_stl2_xml(XMLCONCAT('<abc/>', x_vlt_xml())) FROM x;

-- should not be precalculated
SELECT x_stl2_xml(
  XMLELEMENT(name foo, xmlattributes('bar' as bar), x_vlt_xml())
)
FROM x;

-- should not be precalculated
SELECT x_stl2_xml(XMLFOREST('abc' AS foo, x_vlt_xml() AS bar)) FROM x;

-- should not be precalculated
SELECT x_stl2_xml(XMLPARSE(DOCUMENT x_vlt_text_xml())) FROM x;

-- should not be precalculated
SELECT x_stl2_xml(XMLPARSE(CONTENT x_vlt_text_xml_content())) FROM x;

-- should not be precalculated
SELECT x_stl2_xml(XMLPI(name php, x_vlt_text_xml_instruction_content())) FROM x;

-- should not be precalculated
SELECT x_stl2_xml(XMLROOT(x_vlt_xml(), version '1.0', standalone yes)) FROM x;

-- should not be precalculated
SELECT x_stl2_text(XMLSERIALIZE(DOCUMENT x_vlt_xml() AS text)) FROM x;

-- should not be precalculated
SELECT x_stl2_text(XMLSERIALIZE(CONTENT x_vlt_xml_content() AS text)) FROM x;

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt_xml_content() IS DOCUMENT) FROM x;

SELECT x_stl2_xml(XMLCONCAT('<abc/>', x_stl_xml())) FROM x;

SELECT x_stl2_xml(
  XMLELEMENT(name foo, xmlattributes('bar' as bar), x_stl_xml())
)
FROM x;

SELECT x_stl2_xml(XMLFOREST('abc' AS foo, x_stl_xml() AS bar)) FROM x;

SELECT x_stl2_xml(XMLPARSE(DOCUMENT x_stl_text_xml())) FROM x;

SELECT x_stl2_xml(XMLPARSE(CONTENT x_stl_text_xml_content())) FROM x;

SELECT x_stl2_xml(XMLPI(name php, x_stl_text_xml_instruction_content())) FROM x;

SELECT x_stl2_xml(XMLROOT(x_stl_xml(), version '1.0', standalone yes)) FROM x;

SELECT x_stl2_text(XMLSERIALIZE(DOCUMENT x_stl_xml() AS text)) FROM x;

SELECT x_stl2_text(XMLSERIALIZE(CONTENT x_stl_xml_content() AS text)) FROM x;

SELECT x_stl2_boolean(x_stl_xml_content() IS DOCUMENT) FROM x;

-- NullTest expressions testing

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt() IS NULL) FROM x;

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt() IS NOT NULL) FROM x;

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt_wxyz() IS NULL) FROM x;

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt_wxyz() IS NOT NULL) FROM x;

SELECT x_stl2_boolean(x_stl() IS NULL) FROM x;
SELECT x_stl2_boolean(x_stl() IS NOT NULL) FROM x;

SELECT x_stl2_boolean(x_stl_wxyz() IS NULL) FROM x;
SELECT x_stl2_boolean(x_stl_wxyz() IS NOT NULL) FROM x;

-- BooleanTest expressions testing

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt_boolean() IS TRUE) FROM x;

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt_boolean() IS NOT TRUE) FROM x;

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt_boolean() IS FALSE) FROM x;

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt_boolean() IS NOT FALSE) FROM x;

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt_boolean() IS UNKNOWN) FROM x;

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt_boolean() IS NOT UNKNOWN) FROM x;

SELECT x_stl2_boolean(x_stl2_boolean(TRUE) IS TRUE) FROM x;
SELECT x_stl2_boolean(x_stl2_boolean(TRUE) IS NOT TRUE) FROM x;
SELECT x_stl2_boolean(x_stl2_boolean(TRUE) IS FALSE) FROM x;
SELECT x_stl2_boolean(x_stl2_boolean(TRUE) IS NOT FALSE) FROM x;
SELECT x_stl2_boolean(x_stl2_boolean(NULL) IS UNKNOWN) FROM x;
SELECT x_stl2_boolean(x_stl2_boolean(NULL) IS NOT UNKNOWN) FROM x;

-- Tracking functions testing

SET track_functions TO 'all';

-- Simple functions testing
SELECT x_vlt() FROM x; -- should not be precalculated
SELECT x_stl() FROM x;

-- WHERE clause testing
SELECT x_vlt() FROM x WHERE x_vlt() < x; -- should not be precalculated
SELECT x_stl() FROM x WHERE x_stl() < x;

-- JOIN/ON clause testing

-- should not be precalculated
SELECT * FROM x JOIN generate_series(1, 2) y ON x_vlt() < x;

SELECT * FROM x JOIN generate_series(1, 2) y ON x_stl() < x;

-- Functions with constant arguments testing
SELECT x_vlt2(1) FROM x; -- should not be precalculated
SELECT x_stl2(1) FROM x;

-- Nested functions testing
SELECT x_stl2(x_vlt()) FROM x; -- should not be precalculated
SELECT x_imm2(x_vlt()) FROM x; -- should not be precalculated

SELECT x_stl2(x_stl()) FROM x;
SELECT x_imm2(x_stl()) FROM x;

-- Strict functions testing
SELECT x_stl2_strict(x_vlt()) FROM x; -- should not be precalculated
SELECT x_imm2_strict(x_vlt()) FROM x; -- should not be precalculated

SELECT x_stl2_strict(x_stl2_strict(1)) FROM x;
SELECT x_imm2_strict(x_stl2_strict(1)) FROM x;

-- Strict functions with null arguments testing
SELECT x_stl2_strict(x_stl2(NULL)) FROM x;
SELECT x_imm2_strict(x_stl2(NULL)) FROM x;

-- Operators testing
SELECT 1 === 2 FROM x; -- should not be precalculated
SELECT 1 ==== 2 FROM x;

-- Strict operators testing
SELECT x_stl2_boolean(NULL) ==== TRUE FROM x;
SELECT x_stl2_boolean(NULL) ===== TRUE FROM x;

-- Mixed functions and operators testing
SELECT x_stl2_boolean(1 === 2) FROM x; -- should not be precalculated
SELECT x_stl2_boolean(1 ==== 2) FROM x;

SELECT x_vlt() ==== 1 FROM x; -- should not be precalculated
SELECT x_stl() ==== 1 FROM x;

-- Mixed functions and IS (NOT) DISTINCT FROM expressions testing
DROP OPERATOR = (my_integer, my_integer);
CREATE OPERATOR = (
  PROCEDURE = equal_my_integer_vlt,
  LEFTARG = my_integer,
  RIGHTARG = my_integer
);

-- should not be precalculated
SELECT equal_booleans_stl_strict(
  ('(1)'::my_integer IS DISTINCT FROM '(1)'::my_integer),
  ('(1)'::my_integer IS NOT DISTINCT FROM '(1)'::my_integer)
)
FROM x;

DROP OPERATOR = (my_integer, my_integer);
CREATE OPERATOR = (
  PROCEDURE = equal_my_integer_stl,
  LEFTARG = my_integer,
  RIGHTARG = my_integer
);

SELECT equal_booleans_stl_strict(
  ('(1)'::my_integer IS DISTINCT FROM '(1)'::my_integer),
  ('(1)'::my_integer IS NOT DISTINCT FROM '(1)'::my_integer)
)
FROM x;

-- should not be precalculated
SELECT (x_vlt_my_integer() IS DISTINCT FROM '(1)'::my_integer) FROM x;

-- should not be precalculated
SELECT (x_vlt_my_integer() IS NOT DISTINCT FROM '(1)'::my_integer) FROM x;

SELECT (x_stl_my_integer() IS DISTINCT FROM '(1)'::my_integer) FROM x;
SELECT (x_stl_my_integer() IS NOT DISTINCT FROM '(1)'::my_integer) FROM x;

-- Mixed functions and NULLIF expressions testing
DROP OPERATOR = (my_integer, my_integer);
CREATE OPERATOR = (
  PROCEDURE = equal_my_integer_vlt,
  LEFTARG = my_integer,
  RIGHTARG = my_integer
);

-- should not be precalculated
SELECT equal_my_integer_stl(
  NULLIF('(1)'::my_integer, '(2)'::my_integer),
  NULLIF('(2)'::my_integer, '(2)'::my_integer)
)
FROM x;

DROP OPERATOR = (my_integer, my_integer);
CREATE OPERATOR = (
  PROCEDURE = equal_my_integer_stl,
  LEFTARG = my_integer,
  RIGHTARG = my_integer
);

SELECT equal_my_integer_stl(
  NULLIF('(1)'::my_integer, '(2)'::my_integer),
  NULLIF('(2)'::my_integer, '(2)'::my_integer)
)
FROM x;

-- should not be precalculated
SELECT NULLIF(x_vlt_my_integer(), '(2)'::my_integer) FROM x;

SELECT NULLIF(x_stl_my_integer(), '(2)'::my_integer) FROM x;

-- Mixed functions and "scalar op ANY/ALL (array)" / "scalar IN (2 or more
-- values)" expressions testing

-- should not be precalculated
SELECT x_stl2_boolean(1 === ANY ('{2, 3}')) FROM x;

-- should not be precalculated
SELECT x_stl2_boolean(1 === ALL ('{2, 3}')) FROM x;

DROP OPERATOR = (my_integer, my_integer);
CREATE OPERATOR = (
  PROCEDURE = equal_my_integer_vlt,
  LEFTARG = my_integer,
  RIGHTARG = my_integer
);

-- should not be precalculated
SELECT x_stl2_boolean(
  '(1)'::my_integer IN ('(2)'::my_integer, '(3)'::my_integer)
)
FROM x;

SELECT x_stl2_boolean(1 ==== ANY ('{2, 3}')) FROM x;
SELECT x_stl2_boolean(1 ==== ALL ('{2, 3}')) FROM x;

DROP OPERATOR = (my_integer, my_integer);
CREATE OPERATOR = (
  PROCEDURE = equal_my_integer_stl,
  LEFTARG = my_integer,
  RIGHTARG = my_integer
);

SELECT x_stl2_boolean(
  '(1)'::my_integer IN ('(2)'::my_integer, '(3)'::my_integer)
)
FROM x;

SELECT x_vlt() ==== ANY ('{2, 3}') FROM x; -- should not be precalculated
SELECT x_vlt() ==== ALL ('{2, 3}') FROM x; -- should not be precalculated

SELECT 1 ==== ANY (x_vlt_array_integer()) FROM x; -- should not be precalculated
SELECT 1 ==== ALL (x_vlt_array_integer()) FROM x; -- should not be precalculated

-- should not be precalculated
SELECT x_vlt_my_integer() IN ('(2)'::my_integer, '(3)'::my_integer) FROM x;

SELECT x_stl() ==== ANY ('{2, 3}') FROM x;
SELECT x_stl() ==== ALL ('{2, 3}') FROM x;

SELECT 1 ==== ANY (x_stl_array_integer()) FROM x;
SELECT 1 ==== ALL (x_stl_array_integer()) FROM x;

SELECT x_stl_my_integer() IN ('(2)'::my_integer, '(3)'::my_integer) FROM x;

-- Mixed functions and boolean expressions testing

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt_boolean() AND x_stl2_boolean(TRUE)) FROM x;

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt_boolean() OR x_stl2_boolean(TRUE)) FROM x;

-- should not be precalculated
SELECT x_stl2_boolean(NOT x_vlt_boolean()) FROM x;

SELECT x_stl2_boolean(x_stl2_boolean(TRUE) AND x_stl2_boolean(TRUE)) FROM x;
SELECT x_stl2_boolean(x_stl2_boolean(TRUE) OR x_stl2_boolean(TRUE)) FROM x;
SELECT x_stl2_boolean(NOT x_stl2_boolean(TRUE)) FROM x;

-- Mixed functions and ARRAY[] expressions testing

-- should not be precalculated
SELECT x_stl2_array_integer(ARRAY[x_vlt()]) FROM x;

SELECT x_stl2_array_integer(ARRAY[x_stl()]) FROM x;

-- Mixed functions and array subscripting operations testing

-- should not be precalculated
SELECT x_stl2((x_vlt_array_integer())[x_vlt()]) FROM x;

-- should not be precalculated
SELECT x_stl2((x_vlt_array_integer())[1]) FROM x;

-- should not be precalculated
SELECT x_stl2_array_integer((x_vlt_array_integer())[:]) FROM x;

-- should not be precalculated
SELECT x_stl2(('{1, 2}'::integer[])[x_vlt()]) FROM x;

SELECT x_stl2((x_stl_array_integer())[x_stl()]) FROM x;
SELECT x_stl2((x_stl_array_integer())[1]) FROM x;
SELECT x_stl2_array_integer((x_stl_array_integer())[:]) FROM x;
SELECT x_stl2(('{1, 2}'::integer[])[x_stl()]) FROM x;

-- Mixed functions and FieldSelect expressions testing
SELECT x_stl2((x_vlt_wxyz()).w) FROM x; -- should not be precalculated
SELECT x_stl2((x_vlt_my_integer()).value) FROM x; -- should not be precalculated

SELECT x_stl2((x_stl_wxyz()).w) FROM x;
SELECT x_stl2((x_stl_my_integer()).value) FROM x;

-- Mixed functions and ROW() expressions testing

-- should not be precalculated
SELECT x_stl2_wxyz((x_vlt(), '{2}', TRUE, 3)) FROM x;

SELECT x_stl2_wxyz((x_stl(), '{2}', TRUE, 3)) FROM x;

-- Mixed functions and RelabelType expressions testing
SELECT x_stl2(x_vlt_oid()::integer) FROM x; -- should not be precalculated
SELECT x_stl2(x_stl_oid()::integer) FROM x;

-- Mixed functions and CoerceViaIO expressions testing

-- should not be precalculated
SELECT x_stl2(x_vlt_text_integer()::integer) FROM x;

SELECT x_stl2(x_stl_text_integer()::integer) FROM x;

-- should not be precalculated
SELECT x_stl2_my_integer(x_vlt_text_my_integer()::my_integer) FROM x;

SELECT x_stl2_my_integer(x_stl_text_my_integer()::my_integer) FROM x;

-- Mixed functions and ArrayCoerce expressions testing
-- Binary-coercible types:

-- should not be precalculated
SELECT x_stl2_array_oid(x_vlt_array_integer()::oid[]) FROM x;

SELECT x_stl2_array_oid(x_stl_array_integer()::oid[]) FROM x;

-- Not binary-coercible types:
DROP CAST (my_integer AS integer);
CREATE CAST (my_integer AS integer)
  WITH FUNCTION cast_my_integer_as_integer_vlt;

-- should not be precalculated
SELECT x_stl2_array_integer('{(1), (2)}'::my_integer[]::integer[]) FROM x;

DROP CAST (my_integer AS integer);
CREATE CAST (my_integer AS integer)
  WITH FUNCTION cast_my_integer_as_integer_stl;

SELECT x_stl2_array_integer('{(1), (2)}'::my_integer[]::integer[]) FROM x;

DROP CAST (integer AS my_integer);
CREATE CAST (integer AS my_integer)
  WITH FUNCTION cast_integer_as_my_integer_stl;

-- should not be precalculated
SELECT x_vlt_array_integer()::my_integer[] FROM x;

SELECT x_stl_array_integer()::my_integer[] FROM x;

-- Mixed functions and ConvertRowtypeExpr testing

-- should not be precalculated
SELECT x_stl2_wxyz(x_vlt_wxyz_child()::wxyz_child::wxyz) FROM x;

-- should not be precalculated
SELECT x_stl2_wxyz(x_vlt_wxyz_child2()::wxyz_child2::wxyz) FROM x;

SELECT x_stl2_wxyz(x_stl_wxyz_child()::wxyz_child::wxyz) FROM x;
SELECT x_stl2_wxyz(x_stl_wxyz_child2()::wxyz_child2::wxyz) FROM x;

-- Mixed functions and CASE expressions testing

-- should not be precalculated
SELECT x_stl2(CASE WHEN x_vlt_boolean() THEN x_vlt() ELSE x_vlt() END) FROM x;

-- should not be precalculated
SELECT x_stl2(CASE x_vlt() WHEN x_vlt() THEN x_vlt() ELSE x_vlt() END) FROM x;

SELECT x_stl2(CASE WHEN x_stl2_boolean(TRUE) THEN x_stl() ELSE x_stl() END)
FROM x;

SELECT x_stl2(CASE x_stl() WHEN x_stl() THEN x_stl() ELSE x_stl() END) FROM x;

-- Mixed functions and RowCompareExpr testing

-- should not be precalculated
SELECT x_stl2_boolean((x_vlt(), 2) < (1, 3)) FROM x;

SELECT x_stl2_boolean((x_stl(), 2) < (1, 3)) FROM x;

-- Mixed functions and COALESCE expressions testing

-- should not be precalculated
SELECT x_stl2(COALESCE(NULL, x_vlt2(NULL), 2)) FROM x;

SELECT x_stl2(COALESCE(NULL, x_stl2(NULL), 2)) FROM x;

-- Mixed functions and GREATEST and LEAST functions testing
SELECT x_stl2(GREATEST(2, x_vlt(), 3)) FROM x; -- should not be precalculated
SELECT x_stl2(LEAST(2, x_vlt(), 3)) FROM x; -- should not be precalculated

SELECT x_stl2(GREATEST(2, x_stl(), 3)) FROM x;
SELECT x_stl2(LEAST(2, x_stl(), 3)) FROM x;

-- Mixed functions and Xml expressions testing
-- should not be precalculated
SELECT x_stl2_xml(XMLCONCAT('<abc/>', x_vlt_xml())) FROM x;

-- should not be precalculated
SELECT x_stl2_xml(
  XMLELEMENT(name foo, xmlattributes('bar' as bar), x_vlt_xml())
)
FROM x;

-- should not be precalculated
SELECT x_stl2_xml(XMLFOREST('abc' AS foo, x_vlt_xml() AS bar)) FROM x;

-- should not be precalculated
SELECT x_stl2_xml(XMLPARSE(DOCUMENT x_vlt_text_xml())) FROM x;

-- should not be precalculated
SELECT x_stl2_xml(XMLPARSE(CONTENT x_vlt_text_xml_content())) FROM x;

-- should not be precalculated
SELECT x_stl2_xml(XMLPI(name php, x_vlt_text_xml_instruction_content())) FROM x;

-- should not be precalculated
SELECT x_stl2_xml(XMLROOT(x_vlt_xml(), version '1.0', standalone yes)) FROM x;

-- should not be precalculated
SELECT x_stl2_text(XMLSERIALIZE(DOCUMENT x_vlt_xml() AS text)) FROM x;

-- should not be precalculated
SELECT x_stl2_text(XMLSERIALIZE(CONTENT x_vlt_xml_content() AS text)) FROM x;

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt_xml_content() IS DOCUMENT) FROM x;

SELECT x_stl2_xml(XMLCONCAT('<abc/>', x_stl_xml())) FROM x;

SELECT x_stl2_xml(
  XMLELEMENT(name foo, xmlattributes('bar' as bar), x_stl_xml())
)
FROM x;

SELECT x_stl2_xml(XMLFOREST('abc' AS foo, x_stl_xml() AS bar)) FROM x;

SELECT x_stl2_xml(XMLPARSE(DOCUMENT x_stl_text_xml())) FROM x;

SELECT x_stl2_xml(XMLPARSE(CONTENT x_stl_text_xml_content())) FROM x;

SELECT x_stl2_xml(XMLPI(name php, x_stl_text_xml_instruction_content())) FROM x;

SELECT x_stl2_xml(XMLROOT(x_stl_xml(), version '1.0', standalone yes)) FROM x;

SELECT x_stl2_text(XMLSERIALIZE(DOCUMENT x_stl_xml() AS text)) FROM x;

SELECT x_stl2_text(XMLSERIALIZE(CONTENT x_stl_xml_content() AS text)) FROM x;

SELECT x_stl2_boolean(x_stl_xml_content() IS DOCUMENT) FROM x;

-- Mixed functions and NullTest expressions testing

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt() IS NULL) FROM x;

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt() IS NOT NULL) FROM x;

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt_wxyz() IS NULL) FROM x;

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt_wxyz() IS NOT NULL) FROM x;

SELECT x_stl2_boolean(x_stl() IS NULL) FROM x;
SELECT x_stl2_boolean(x_stl() IS NOT NULL) FROM x;

SELECT x_stl2_boolean(x_stl_wxyz() IS NULL) FROM x;
SELECT x_stl2_boolean(x_stl_wxyz() IS NOT NULL) FROM x;

-- Mixed functions and BooleanTest expressions testing

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt_boolean() IS TRUE) FROM x;

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt_boolean() IS NOT TRUE) FROM x;

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt_boolean() IS FALSE) FROM x;

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt_boolean() IS NOT FALSE) FROM x;

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt_boolean() IS UNKNOWN) FROM x;

-- should not be precalculated
SELECT x_stl2_boolean(x_vlt_boolean() IS NOT UNKNOWN) FROM x;

SELECT x_stl2_boolean(x_stl2_boolean(TRUE) IS TRUE) FROM x;
SELECT x_stl2_boolean(x_stl2_boolean(TRUE) IS NOT TRUE) FROM x;
SELECT x_stl2_boolean(x_stl2_boolean(TRUE) IS FALSE) FROM x;
SELECT x_stl2_boolean(x_stl2_boolean(TRUE) IS NOT FALSE) FROM x;
SELECT x_stl2_boolean(x_stl2_boolean(NULL) IS UNKNOWN) FROM x;
SELECT x_stl2_boolean(x_stl2_boolean(NULL) IS NOT UNKNOWN) FROM x;

SET track_functions TO DEFAULT;

-- ROW() expressions with dropped columns testing

ALTER TABLE wxyz DROP COLUMN z;

-- Update some functions
CREATE OR REPLACE FUNCTION public.x_stl2_wxyz (
  wxyz
)
RETURNS wxyz STABLE AS
$body$
BEGIN
  RAISE NOTICE 's2 wxyz';
  RETURN $1;
END;
$body$
LANGUAGE 'plpgsql';

-- ROW() expressions testing
SELECT x_stl2_wxyz((1, '{2}', TRUE)) FROM x;
SELECT x_stl2_wxyz(ROW(1, '{2}', TRUE)) FROM x;
SELECT x_stl2_wxyz((1, '{2}', TRUE)::wxyz) FROM x;

-- Mixed functions and ROW() expressions testing

-- should not be precalculated
SELECT x_stl2_wxyz((x_vlt(), '{2}', TRUE)) FROM x;

SELECT x_stl2_wxyz((x_stl(), '{2}', TRUE)) FROM x;

-- PL/pgSQL Simple expressions
-- Make sure precalculated stable functions can't be simple expressions: these
-- expressions are only initialized once per transaction and then executed
-- multiple times.

BEGIN;
SELECT simple();
INSERT INTO x VALUES (5);
SELECT simple();
ROLLBACK;

-- Prepared statements testing

PREPARE test_x_imm2 (integer) AS SELECT x_imm2(x_imm2($1)) FROM x;
EXPLAIN (COSTS OFF) EXECUTE test_x_imm2(2);
EXPLAIN (COSTS OFF) EXECUTE test_x_imm2(2);
EXPLAIN (COSTS OFF) EXECUTE test_x_imm2(2);
EXPLAIN (COSTS OFF) EXECUTE test_x_imm2(2);
EXPLAIN (COSTS OFF) EXECUTE test_x_imm2(2);
EXPLAIN (COSTS OFF) EXECUTE test_x_imm2(2);
EXPLAIN (COSTS OFF) EXECUTE test_x_imm2(2);
EXPLAIN (COSTS OFF) EXECUTE test_x_imm2(2);
EXPLAIN (COSTS OFF) EXECUTE test_x_imm2(2);
EXPLAIN (COSTS OFF) EXECUTE test_x_imm2(2);

-- Drop tables for testing

DROP TABLE x;

DROP FUNCTION x_vlt_wxyz, x_vlt_wxyz_child, x_vlt_wxyz_child2;
DROP FUNCTION x_stl_wxyz, x_stl_wxyz_child, x_stl_wxyz_child2, x_stl2_wxyz;
DROP TABLE wxyz, wxyz_child, wxyz_child2;

DROP FUNCTION x_stl2_no_columns;
DROP TABLE no_columns, no_columns_child, no_columns_child2;
