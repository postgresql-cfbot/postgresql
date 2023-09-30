# Test session variables memory cleanup for sinval

setup
{
    CREATE VARIABLE myvar AS text;
}

teardown
{
    DROP VARIABLE IF EXISTS myvar;
}

session s1
step s1		{ BEGIN; }
step let	{ LET myvar = 'test'; }
step val	{ SELECT myvar; }
step dbg	{ SELECT schema, name, removed FROM pg_session_variables(); }
step sr1	{ ROLLBACK; }

session s2
step drop		{ DROP VARIABLE myvar; }
step create		{ CREATE VARIABLE myvar AS text; }

session s3
step s3			{ BEGIN; }
step let3		{ LET myvar3 = 'test'; }
step o_c_d		{ CREATE TEMP VARIABLE myvar_o_c_d AS text ON COMMIT DROP; }
step o_eox_r	{ CREATE VARIABLE myvar_o_eox_r AS text ON TRANSACTION END RESET; LET myvar_o_eox_r = 'test'; }
step create4	{ CREATE VARIABLE myvar4 AS text; }
step let4		{ LET myvar4 = 'test'; }
step drop4		{ DROP VARIABLE myvar4; }
step inval3		{ SELECT COUNT(*) >= 0 FROM pg_foreign_table; }
step discard	{ DISCARD VARIABLES; }
step sc3		{ COMMIT; }
step clean		{ DROP VARIABLE myvar_o_eox_r; }
step state		{ SELECT varname FROM pg_variable; }

session s4
step create3	{ CREATE VARIABLE myvar3 AS text; }
step drop3		{ DROP VARIABLE myvar3; }

# Concurrent drop of a known variable should lead to an error
permutation let val drop val
# Same, but with an explicit transaction
permutation let val s1 drop val sr1
# Concurrent drop/create of a known variable should lead to empty variable
permutation let val dbg drop create dbg val
# Concurrent drop/create of a known variable should lead to empty variable
# We need a transaction to make sure that we won't accept invalidation when
# calling the dbg step after the concurrent drop
permutation let val s1 dbg drop create dbg val sr1
# test for DISCARD ALL when all internal queues have actions registered
permutation create3 let3 s3 o_c_d o_eox_r create4 let4 drop4 drop3 inval3 discard sc3 clean state
