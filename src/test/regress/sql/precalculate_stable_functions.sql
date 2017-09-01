--
-- PRECALCULATE STABLE FUNCTIONS
--

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

-- Create operators for testing

CREATE operator === (PROCEDURE = equal_integers_vlt, LEFTARG = integer, RIGHTARG = integer);
CREATE operator ==== (PROCEDURE = equal_integers_stl, LEFTARG = integer, RIGHTARG = integer);
CREATE operator ===== (PROCEDURE = equal_integers_imm, LEFTARG = integer, RIGHTARG = integer);
CREATE operator ====== (PROCEDURE = equal_booleans_stl_strict, LEFTARG = boolean, RIGHTARG = boolean);

-- Simple functions testing

SELECT x_vlt() FROM generate_series(1, 3) x; -- should not be precalculated
SELECT x_stl() FROM generate_series(1, 3) x;

SELECT x_vlt() FROM generate_series(1, 4) x WHERE x_vlt() < x; -- should not be precalculated
SELECT x_stl() FROM generate_series(1, 4) x WHERE x_stl() < x;

-- Functions with constant arguments and nested functions testing

SELECT x_stl2(x_vlt()) FROM generate_series(1, 4) x; -- should not be precalculated
SELECT x_imm2(x_vlt()) FROM generate_series(1, 4) x; -- should not be precalculated

SELECT x_stl2(x_stl2(1)) FROM generate_series(1, 4) x;
SELECT x_imm2(x_stl2(1)) FROM generate_series(1, 4) x;

-- Strict functions testing

SELECT x_stl2_strict(x_vlt()) FROM generate_series(1, 4) x; -- should not be precalculated
SELECT x_imm2_strict(x_vlt()) FROM generate_series(1, 4) x; -- should not be precalculated

SELECT x_stl2_strict(x_stl2_strict(1)) FROM generate_series(1, 4) x;
SELECT x_imm2_strict(x_stl2_strict(1)) FROM generate_series(1, 4) x;

-- Strict functions with null arguments testing

SELECT x_stl2_strict(x_stl2(NULL)) FROM generate_series(1, 4) x;
SELECT x_imm2_strict(x_stl2(NULL)) FROM generate_series(1, 4) x;

-- Operators testing

SELECT 1 === 2 FROM generate_series(1, 4) x; -- should not be precalculated
SELECT 1 ==== 2 FROM generate_series(1, 4) x;
SELECT 1 ===== 2 FROM generate_series(1, 4) x;

-- Nested and strict operators testing

SELECT (x_vlt() ==== 2) ====== (x_vlt() ===== 3) FROM generate_series(1, 4) x; -- should not be precalculated
SELECT (1 ==== 2) ====== (3 ==== 3) FROM generate_series(1, 4) x;
SELECT x_stl2_boolean(NULL) ====== (3 ==== 3) FROM generate_series(1, 4) x;

-- Mixed functions and operators testing

SELECT x_vlt() ==== 2 FROM generate_series(1, 4) x; -- should not be precalculated
SELECT x_vlt() ===== 2 FROM generate_series(1, 4) x; -- should not be precalculated

SELECT x_stl() ==== x_stl() FROM generate_series(1, 4) x;
SELECT x_stl2_boolean(1 ==== 2) FROM generate_series(1, 4) x;

-- Tracking functions testing

SET track_functions TO 'all';

SELECT x_vlt() FROM generate_series(1, 3) x; -- should not be precalculated
SELECT x_stl() FROM generate_series(1, 3) x;

SELECT x_vlt() FROM generate_series(1, 4) x WHERE x_vlt() < x; -- should not be precalculated
SELECT x_stl() FROM generate_series(1, 4) x WHERE x_stl() < x;

SELECT x_stl2(x_vlt()) FROM generate_series(1, 4) x; -- should not be precalculated
SELECT x_imm2(x_vlt()) FROM generate_series(1, 4) x; -- should not be precalculated

SELECT x_stl2(x_stl2(1)) FROM generate_series(1, 4) x;
SELECT x_imm2(x_stl2(1)) FROM generate_series(1, 4) x;

SELECT x_stl2_strict(x_vlt()) FROM generate_series(1, 4) x; -- should not be precalculated
SELECT x_imm2_strict(x_vlt()) FROM generate_series(1, 4) x; -- should not be precalculated

SELECT x_stl2_strict(x_stl2_strict(1)) FROM generate_series(1, 4) x;
SELECT x_imm2_strict(x_stl2_strict(1)) FROM generate_series(1, 4) x;

SELECT x_stl2_strict(x_stl2(NULL)) FROM generate_series(1, 4) x;
SELECT x_imm2_strict(x_stl2(NULL)) FROM generate_series(1, 4) x;

SELECT 1 === 2 FROM generate_series(1, 4) x; -- should not be precalculated
SELECT 1 ==== 2 FROM generate_series(1, 4) x;
SELECT 1 ===== 2 FROM generate_series(1, 4) x;

SELECT (x_vlt() ==== 2) ====== (x_vlt() ===== 3) FROM generate_series(1, 4) x; -- should not be precalculated
SELECT (1 ==== 2) ====== (3 ==== 3) FROM generate_series(1, 4) x;
SELECT x_stl2_boolean(NULL) ====== (3 ==== 3) FROM generate_series(1, 4) x;

SELECT x_vlt() ==== 2 FROM generate_series(1, 4) x; -- should not be precalculated
SELECT x_vlt() ===== 2 FROM generate_series(1, 4) x; -- should not be precalculated

SELECT x_stl() ==== x_stl() FROM generate_series(1, 4) x;
SELECT x_stl2_boolean(1 ==== 2) FROM generate_series(1, 4) x;

SET track_functions TO DEFAULT;