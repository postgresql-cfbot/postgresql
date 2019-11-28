CREATE EXTENSION ltree_plpython2u CASCADE;


CREATE FUNCTION test1(val ltree) RETURNS int
LANGUAGE plpythonu
TRANSFORM FOR TYPE ltree
AS $$
plpy.info(repr(val))
return len(val)
$$;

SELECT test1('aa.bb.cc'::ltree);


CREATE FUNCTION test1n(val ltree) RETURNS int
LANGUAGE plpython2u
TRANSFORM FOR TYPE ltree
AS $$
plpy.info(repr(val))
return len(val)
$$;

SELECT test1n('aa.bb.cc'::ltree);
