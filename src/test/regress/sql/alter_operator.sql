CREATE FUNCTION alter_op_test_fn(boolean, boolean)
RETURNS boolean AS $$ SELECT NULL::BOOLEAN; $$ LANGUAGE sql IMMUTABLE;

CREATE FUNCTION customcontsel(internal, oid, internal, integer)
RETURNS float8 AS 'contsel' LANGUAGE internal STABLE STRICT;

CREATE OPERATOR === (
    LEFTARG = boolean,
    RIGHTARG = boolean,
    PROCEDURE = alter_op_test_fn,
    COMMUTATOR = ===,
    NEGATOR = !==,
    RESTRICT = customcontsel,
    JOIN = contjoinsel,
    HASHES, MERGES
);

SELECT pg_describe_object(refclassid,refobjid,refobjsubid) as ref, deptype
FROM pg_depend
WHERE classid = 'pg_operator'::regclass AND
      objid = '===(bool,bool)'::regoperator
ORDER BY 1;

--
-- test reset and set restrict and join
--

ALTER OPERATOR === (boolean, boolean) SET (RESTRICT = NONE);
ALTER OPERATOR === (boolean, boolean) SET (JOIN = NONE);

SELECT oprrest, oprjoin FROM pg_operator WHERE oprname = '==='
  AND oprleft = 'boolean'::regtype AND oprright = 'boolean'::regtype;

SELECT pg_describe_object(refclassid,refobjid,refobjsubid) as ref, deptype
FROM pg_depend
WHERE classid = 'pg_operator'::regclass AND
      objid = '===(bool,bool)'::regoperator
ORDER BY 1;

ALTER OPERATOR === (boolean, boolean) SET (RESTRICT = contsel);
ALTER OPERATOR === (boolean, boolean) SET (JOIN = contjoinsel);

SELECT oprrest, oprjoin FROM pg_operator WHERE oprname = '==='
  AND oprleft = 'boolean'::regtype AND oprright = 'boolean'::regtype;

SELECT pg_describe_object(refclassid,refobjid,refobjsubid) as ref, deptype
FROM pg_depend
WHERE classid = 'pg_operator'::regclass AND
      objid = '===(bool,bool)'::regoperator
ORDER BY 1;

ALTER OPERATOR === (boolean, boolean) SET (RESTRICT = NONE, JOIN = NONE);

SELECT oprrest, oprjoin FROM pg_operator WHERE oprname = '==='
  AND oprleft = 'boolean'::regtype AND oprright = 'boolean'::regtype;

SELECT pg_describe_object(refclassid,refobjid,refobjsubid) as ref, deptype
FROM pg_depend
WHERE classid = 'pg_operator'::regclass AND
      objid = '===(bool,bool)'::regoperator
ORDER BY 1;

ALTER OPERATOR === (boolean, boolean) SET (RESTRICT = customcontsel, JOIN = contjoinsel);

SELECT oprrest, oprjoin FROM pg_operator WHERE oprname = '==='
  AND oprleft = 'boolean'::regtype AND oprright = 'boolean'::regtype;

SELECT pg_describe_object(refclassid,refobjid,refobjsubid) as ref, deptype
FROM pg_depend
WHERE classid = 'pg_operator'::regclass AND
      objid = '===(bool,bool)'::regoperator
ORDER BY 1;

-- test cannot set non existant function
ALTER OPERATOR === (boolean, boolean) SET (RESTRICT = non_existent_func);
ALTER OPERATOR === (boolean, boolean) SET (JOIN = non_existent_func);

-- test non-lowercase quoted identifiers invalid
ALTER OPERATOR & (bit, bit) SET ("Restrict" = _int_contsel, "Join" = _int_contjoinsel);

--
-- test must be owner of operator to ALTER OPERATOR.
--
CREATE USER regress_alter_op_user;
SET SESSION AUTHORIZATION regress_alter_op_user;

ALTER OPERATOR === (boolean, boolean) SET (RESTRICT = NONE);

RESET SESSION AUTHORIZATION;

--
-- test set commutator, negator, hashes, and merges which can only be set if not
-- already set
--

-- for these tests create operators with different left and right types so that
-- we can validate that function signatures are handled correctly

CREATE FUNCTION alter_op_test_fn_bool_real(boolean, real)
RETURNS boolean AS $$ SELECT NULL::BOOLEAN; $$ LANGUAGE sql IMMUTABLE;

CREATE FUNCTION alter_op_test_fn_real_bool(real, boolean)
RETURNS boolean AS $$ SELECT NULL::BOOLEAN; $$ LANGUAGE sql IMMUTABLE;

-- operator
CREATE OPERATOR === (
    LEFTARG = boolean,
    RIGHTARG = real,
    PROCEDURE = alter_op_test_fn_bool_real
);

-- commutator
CREATE OPERATOR ==== (
    LEFTARG = real,
    RIGHTARG = boolean,
    PROCEDURE = alter_op_test_fn_real_bool
);

-- negator
CREATE OPERATOR !==== (
    LEFTARG = boolean,
    RIGHTARG = real,
    PROCEDURE = alter_op_test_fn_bool_real
);

-- test no-op setting already false hashes and merges to false works
ALTER OPERATOR === (boolean, real) SET (HASHES = false);
ALTER OPERATOR === (boolean, real) SET (MERGES = false);

-- validate still false after no-op
SELECT oprcanhash, oprcanmerge FROM pg_operator WHERE oprname = '==='
  AND oprleft = 'boolean'::regtype AND oprright = 'real'::regtype;

-- test cannot set commutator or negator without owning them
SET SESSION AUTHORIZATION regress_alter_op_user;

-- we need need a new operator owned by regress_alter_op_user so that we are
-- allowed to alter it. ==== and !==== are owned by the test user so we expect
-- the alters below to fail.
CREATE OPERATOR ===@@@ (
    LEFTARG = boolean,
    RIGHTARG = real,
    PROCEDURE = alter_op_test_fn_bool_real
);

ALTER OPERATOR ===@@@ (boolean, real) SET (COMMUTATOR= ====);
ALTER OPERATOR ===@@@ (boolean, real) SET (NEGATOR = !====);

-- validate operator is unchanged and commutator and negator are unset
SELECT oprcom, oprnegate FROM pg_operator WHERE oprname = '===@@@'
  AND oprleft = 'boolean'::regtype AND oprright = 'real'::regtype;

DROP OPERATOR ===@@@ (boolean, real);

RESET SESSION AUTHORIZATION;

-- test cannot set self negator
ALTER OPERATOR === (boolean, real) SET (NEGATOR = ===);

-- validate no changes made
SELECT oprcanmerge, oprcanhash, oprcom, oprnegate
FROM pg_operator WHERE oprname = '==='
  AND oprleft = 'boolean'::regtype AND oprright = 'real'::regtype;

-- test set hashes
ALTER OPERATOR === (boolean, real) SET (HASHES);
SELECT oprcanhash FROM pg_operator WHERE oprname = '==='
  AND oprleft = 'boolean'::regtype AND oprright = 'real'::regtype;

-- test set merges
ALTER OPERATOR === (boolean, real) SET (MERGES);
SELECT oprcanmerge FROM pg_operator WHERE oprname = '==='
  AND oprleft = 'boolean'::regtype AND oprright = 'real'::regtype;

-- test set commutator
ALTER OPERATOR === (boolean, real) SET (COMMUTATOR = ====);

-- validate that the commutator has been set on both the operator and commutator,
-- that they reference each other, and that the operator used is the existing
-- one we created and not a new shell operator
SELECT op.oprname AS operator_name, com.oprname AS commutator_name,
  com.oprcode AS commutator_func
  FROM pg_operator op
  INNER JOIN pg_operator com ON (op.oid = com.oprcom AND op.oprcom = com.oid)
  WHERE op.oprname = '==='
  AND op.oprleft = 'boolean'::regtype AND op.oprright = 'real'::regtype;

-- test set negator
ALTER OPERATOR === (boolean, real) SET (NEGATOR = !====);

-- validate that the negator has been set on both the operator and negator, that
-- they reference each other, and that the operator used is the existing one we
-- created and not a new shell operator
SELECT op.oprname AS operator_name, neg.oprname AS negator_name,
  neg.oprcode AS negator_func
  FROM pg_operator op
  INNER JOIN pg_operator neg ON (op.oid = neg.oprnegate AND op.oprnegate = neg.oid)
  WHERE op.oprname = '==='
  AND op.oprleft = 'boolean'::regtype AND op.oprright = 'real'::regtype;

-- validate that the final state of the operator is as we expect
SELECT oprcanmerge, oprcanhash,
       pg_describe_object('pg_operator'::regclass, oprcom, 0) AS commutator,
       pg_describe_object('pg_operator'::regclass, oprnegate, 0) AS negator
  FROM pg_operator WHERE oprname = '==='
  AND oprleft = 'boolean'::regtype AND oprright = 'real'::regtype;

-- test no-op set 'succeeds'
ALTER OPERATOR === (boolean, real) SET (NEGATOR = !====);
ALTER OPERATOR === (boolean, real) SET (COMMUTATOR = ====);
ALTER OPERATOR === (boolean, real) SET (HASHES);
ALTER OPERATOR === (boolean, real) SET (MERGES);

-- test cannot change commutator, negator, hashes, and merges when already set
ALTER OPERATOR === (boolean, real) SET (COMMUTATOR = =);
ALTER OPERATOR === (boolean, real) SET (NEGATOR = =);
ALTER OPERATOR === (boolean, real) SET (HASHES = false);
ALTER OPERATOR === (boolean, real) SET (MERGES = false);

-- validate no changes made
SELECT oprcanmerge, oprcanhash,
       pg_describe_object('pg_operator'::regclass, oprcom, 0) AS commutator,
       pg_describe_object('pg_operator'::regclass, oprnegate, 0) AS negator
  FROM pg_operator WHERE oprname = '==='
  AND oprleft = 'boolean'::regtype AND oprright = 'real'::regtype;

--
-- test setting undefined operator creates shell operator (matches the
-- behaviour of CREATE OPERATOR)
--

DROP OPERATOR === (boolean, real);
CREATE OPERATOR === (
    LEFTARG = boolean,
    RIGHTARG = real,
    PROCEDURE = alter_op_test_fn_bool_real
);

ALTER OPERATOR === (boolean, real) SET (COMMUTATOR = ===@@@);
ALTER OPERATOR === (boolean, real) SET (NEGATOR = !===@@@);

-- validate that shell operators are created for the commutator and negator with
-- the commutator having reversed args and the negator matching the operator.
-- The shell operators should have an empty function below.
SELECT pg_describe_object('pg_operator'::regclass, op.oid, 0) AS operator,
       pg_describe_object('pg_operator'::regclass, com.oid, 0) AS commutator,
       com.oprcode AS commutator_func,
       pg_describe_object('pg_operator'::regclass, neg.oid, 0) AS negator,
       neg.oprcode AS negator_func
  FROM pg_operator op
  INNER JOIN pg_operator com ON (op.oid = com.oprcom AND op.oprcom = com.oid)
  INNER JOIN pg_operator neg ON (op.oid = neg.oprnegate AND op.oprnegate = neg.oid)
  WHERE op.oprname = '===' AND com.oprname = '===@@@' AND neg.oprname = '!===@@@'
  AND op.oprleft = 'boolean'::regtype AND op.oprright = 'real'::regtype;


--
-- test setting self commutator
--

DROP OPERATOR === (boolean, boolean);
CREATE OPERATOR === (
    LEFTARG = boolean,
    RIGHTARG = boolean,
    PROCEDURE = alter_op_test_fn
);

ALTER OPERATOR === (boolean, boolean) SET (COMMUTATOR = ===);

-- validate that the oprcom is the operator oid
SELECT oprname FROM pg_operator 
  WHERE oprname = '===' AND oid = oprcom
  AND oprleft = 'boolean'::regtype AND oprright = 'boolean'::regtype;

--
-- Clean up
--

DROP USER regress_alter_op_user;

DROP OPERATOR === (boolean, boolean);
DROP OPERATOR === (boolean, real);
DROP OPERATOR ==== (real, boolean);
DROP OPERATOR !==== (boolean, real);
DROP OPERATOR ===@@@ (real, boolean);
DROP OPERATOR !===@@@(boolean, real);

DROP FUNCTION customcontsel(internal, oid, internal, integer);
DROP FUNCTION alter_op_test_fn(boolean, boolean);
DROP FUNCTION alter_op_test_fn_bool_real(boolean, real);
DROP FUNCTION alter_op_test_fn_real_bool(real, boolean);
