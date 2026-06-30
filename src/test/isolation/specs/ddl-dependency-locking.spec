# Test that concurrent DROP and CREATE commands do not leave behind
# references to non-existent objects.

setup
{
	CREATE SCHEMA testschema;
	CREATE SCHEMA alterschema;
	CREATE TYPE public.foo as enum ('one', 'two');
	CREATE TYPE public.footab as enum ('three', 'four');
	CREATE DOMAIN id AS int;
	CREATE FUNCTION f() RETURNS int LANGUAGE SQL RETURN 1;
	CREATE FUNCTION public.falter() RETURNS int LANGUAGE SQL RETURN 1;
	CREATE FOREIGN DATA WRAPPER fdw_wrapper;
	CREATE ROLE role_fdw;
	CREATE ROLE role_user LOGIN;
	GRANT USAGE ON FOREIGN DATA WRAPPER fdw_wrapper TO role_fdw;
	GRANT role_fdw TO role_user;
	CREATE ROLE regress_dependency;
	CREATE FUNCTION spi_func(text[], oid) RETURNS void
	  LANGUAGE plpgsql AS $$ BEGIN EXECUTE 'CREATE TEMP TABLE IF NOT EXISTS validator_side_effect(x int)'; END; $$;
	CREATE FUNCTION spi_func_rollback(text[], oid) RETURNS void
	  LANGUAGE plpgsql AS $$
	  BEGIN
	    BEGIN
	      EXECUTE 'CREATE TEMP TABLE validator_will_rollback(x int)';
	      RAISE EXCEPTION 'force rollback';
	    EXCEPTION WHEN OTHERS THEN
	      NULL;
	    END;
	  END; $$;
	CREATE FOREIGN DATA WRAPPER fdw_spi_func VALIDATOR spi_func;
	CREATE FOREIGN DATA WRAPPER fdw_spi_rollback VALIDATOR spi_func_rollback;
	GRANT USAGE ON FOREIGN DATA WRAPPER fdw_spi_func TO role_fdw;
	GRANT USAGE ON FOREIGN DATA WRAPPER fdw_spi_rollback TO role_fdw;
	CREATE FOREIGN DATA WRAPPER fdw_trigger;
	GRANT USAGE ON FOREIGN DATA WRAPPER fdw_trigger TO role_fdw;
	CREATE FUNCTION trg_create_server() RETURNS trigger
	  LANGUAGE plpgsql AS $$ BEGIN EXECUTE 'CREATE SERVER srv_from_trigger FOREIGN DATA WRAPPER fdw_trigger'; RETURN NEW; END; $$;
	CREATE TABLE trigger_tbl(a int);
	GRANT INSERT ON trigger_tbl TO role_user;
	CREATE TRIGGER trg BEFORE INSERT ON trigger_tbl
	  FOR EACH ROW EXECUTE FUNCTION trg_create_server();
	CREATE FOREIGN DATA WRAPPER fdw_trigger_spi_func VALIDATOR spi_func;
	GRANT USAGE ON FOREIGN DATA WRAPPER fdw_trigger_spi_func TO role_fdw;
	CREATE FUNCTION trg_create_server_spi_func() RETURNS trigger
	  LANGUAGE plpgsql AS $$ BEGIN EXECUTE 'CREATE SERVER srv_from_trigger_spi FOREIGN DATA WRAPPER fdw_trigger_spi_func'; RETURN NEW; END; $$;
	CREATE TABLE trigger_spi_tbl(a int);
	GRANT INSERT ON trigger_spi_tbl TO role_user;
	CREATE TRIGGER trg BEFORE INSERT ON trigger_spi_tbl
	  FOR EACH ROW EXECUTE FUNCTION trg_create_server_spi_func();
	CREATE FOREIGN DATA WRAPPER fdw_inner;
	GRANT USAGE ON FOREIGN DATA WRAPPER fdw_inner TO role_fdw;
	CREATE FUNCTION spi_func_create_server(text[], oid) RETURNS void
	  LANGUAGE plpgsql AS $$
	  BEGIN
	    IF $2 = 'pg_catalog.pg_foreign_server'::regclass::oid THEN
	      EXECUTE 'CREATE SERVER srv_nested_inner FOREIGN DATA WRAPPER fdw_inner';
	    END IF;
	  END; $$;
	CREATE FOREIGN DATA WRAPPER fdw_outer VALIDATOR spi_func_create_server;
	GRANT USAGE ON FOREIGN DATA WRAPPER fdw_outer TO role_fdw;
	CREATE ROLE role_fdw_inner;
	GRANT USAGE ON FOREIGN DATA WRAPPER fdw_inner TO role_fdw_inner;
	GRANT role_fdw_inner TO role_user;
	CREATE FUNCTION spi_func_create_server_rollback(text[], oid) RETURNS void
	  LANGUAGE plpgsql AS $$
	  BEGIN
	    IF $2 = 'pg_catalog.pg_foreign_server'::regclass::oid THEN
	      BEGIN
	        EXECUTE 'CREATE SERVER srv_nested_rollback FOREIGN DATA WRAPPER fdw_inner';
	        RAISE EXCEPTION 'force rollback';
	      EXCEPTION WHEN OTHERS THEN
	        NULL;
	      END;
	    END IF;
	  END; $$;
	CREATE FOREIGN DATA WRAPPER fdw_outer_rollback VALIDATOR spi_func_create_server_rollback;
	GRANT USAGE ON FOREIGN DATA WRAPPER fdw_outer_rollback TO role_fdw;
}

teardown
{
	DROP FUNCTION IF EXISTS testschema.foo();
	DROP FUNCTION IF EXISTS fooargtype(num foo);
	DROP FUNCTION IF EXISTS footrettype();
	DROP FUNCTION IF EXISTS foofunc();
	DROP FUNCTION IF EXISTS public.falter();
	DROP FUNCTION IF EXISTS alterschema.falter();
	DROP DOMAIN IF EXISTS idid;
	DROP SERVER IF EXISTS srv_fdw_wrapper;
	DROP SERVER IF EXISTS srv_role_revoked;
	DROP SERVER IF EXISTS srv_spi_func;
	DROP SERVER IF EXISTS srv_spi_rollback;
	DROP SERVER IF EXISTS srv_from_trigger;
	DROP SERVER IF EXISTS srv_from_trigger_spi;
	DROP SERVER IF EXISTS srv_from_spi;
	DROP SERVER IF EXISTS srv_nested_inner;
	DROP SERVER IF EXISTS srv_nested_rollback;
	DROP SERVER IF EXISTS srv_outer;
	DROP SERVER IF EXISTS srv_outer_rollback;
	DROP TABLE IF EXISTS tabtype;
	DROP TABLE IF EXISTS trigger_tbl;
	DROP TABLE IF EXISTS trigger_spi_tbl;
	DROP SCHEMA IF EXISTS testschema;
	DROP SCHEMA IF EXISTS alterschema;
	DROP TYPE IF EXISTS public.foo;
	DROP TYPE IF EXISTS public.footab;
	DROP DOMAIN IF EXISTS id;
	DROP FUNCTION IF EXISTS f();
	DROP FOREIGN DATA WRAPPER IF EXISTS fdw_wrapper;
	DROP FOREIGN DATA WRAPPER IF EXISTS fdw_spi_func;
	DROP FOREIGN DATA WRAPPER IF EXISTS fdw_spi_rollback;
	DROP FOREIGN DATA WRAPPER IF EXISTS fdw_trigger;
	DROP FOREIGN DATA WRAPPER IF EXISTS fdw_trigger_spi_func;
	DROP FOREIGN DATA WRAPPER IF EXISTS fdw_inner;
	DROP FOREIGN DATA WRAPPER IF EXISTS fdw_outer;
	DROP FOREIGN DATA WRAPPER IF EXISTS fdw_outer_rollback;
	DROP FUNCTION IF EXISTS spi_func(text[], oid);
	DROP FUNCTION IF EXISTS spi_func_rollback(text[], oid);
	DROP FUNCTION IF EXISTS spi_func_create_server(text[], oid);
	DROP FUNCTION IF EXISTS spi_func_create_server_rollback(text[], oid);
	DROP FUNCTION IF EXISTS trg_create_server();
	DROP FUNCTION IF EXISTS trg_create_server_spi_func();
	DROP ROLE IF EXISTS role_user;
	DROP ROLE IF EXISTS role_fdw;
	DROP ROLE IF EXISTS role_fdw_inner;
	DROP ROLE regress_dependency;
}

session "s1"

step "s1_begin" { BEGIN; }
step "s1_create_function_in_schema" { CREATE FUNCTION testschema.foo() RETURNS int AS 'select 1' LANGUAGE sql; }
step "s1_create_function_with_argtype" { CREATE FUNCTION fooargtype(num foo) RETURNS int AS 'select 1' LANGUAGE sql; }
step "s1_create_function_with_rettype" { CREATE FUNCTION footrettype() RETURNS id LANGUAGE sql RETURN 1; }
step "s1_create_function_with_function" { CREATE FUNCTION foofunc() RETURNS int LANGUAGE SQL RETURN f() + 1; }
step "s1_alter_function_owner" { ALTER FUNCTION public.falter() OWNER TO regress_dependency; }
step "s1_alter_function_schema" { ALTER FUNCTION public.falter() SET SCHEMA alterschema; }
step "s1_create_domain_with_domain" { CREATE DOMAIN idid as id; }
step "s1_create_table_with_type" { CREATE TABLE tabtype(a footab); }
step "s1_create_server_with_fdw_wrapper" { CREATE SERVER srv_fdw_wrapper FOREIGN DATA WRAPPER fdw_wrapper; }
step "s1_create_server_as_role_user" { SET ROLE role_user; CREATE SERVER srv_role_revoked FOREIGN DATA WRAPPER fdw_wrapper; RESET ROLE; }
step "s1_create_server_spi_func" { SET ROLE role_user; CREATE SERVER srv_spi_func FOREIGN DATA WRAPPER fdw_spi_func; RESET ROLE; }
step "s1_create_server_spi_rollback" { SET ROLE role_user; CREATE SERVER srv_spi_rollback FOREIGN DATA WRAPPER fdw_spi_rollback; RESET ROLE; }
step "s1_insert_trigger_ddl" { SET ROLE role_user; INSERT INTO trigger_tbl VALUES (1); RESET ROLE; }
step "s1_insert_trigger_spi_ddl" { SET ROLE role_user; INSERT INTO trigger_spi_tbl VALUES (1); RESET ROLE; }
step "s1_create_server_nested_toctou" { SET ROLE role_user; CREATE SERVER srv_outer FOREIGN DATA WRAPPER fdw_outer; RESET ROLE; }
step "s1_create_server_nested_rollback" { SET ROLE role_user; CREATE SERVER srv_outer_rollback FOREIGN DATA WRAPPER fdw_outer_rollback; RESET ROLE; }
step "s1_commit" { COMMIT; }

session "s2"

step "s2_begin" { BEGIN; }
step "s2_drop_schema" { DROP SCHEMA testschema; }
step "s2_drop_alterschema" { DROP SCHEMA alterschema; }
step "s2_drop_foo_type" { DROP TYPE public.foo; }
step "s2_drop_foo_rettype" { DROP DOMAIN id; }
step "s2_drop_footab_type" { DROP TYPE public.footab; }
step "s2_drop_function_f" { DROP FUNCTION f(); }
step "s2_drop_domain_id" { DROP DOMAIN id; }
step "s2_drop_fdw_wrapper" { DROP FOREIGN DATA WRAPPER fdw_wrapper RESTRICT; }
step "s2_drop_fdw_spi_func" { DROP FOREIGN DATA WRAPPER fdw_spi_func RESTRICT; }
step "s2_drop_fdw_spi_rollback" { DROP FOREIGN DATA WRAPPER fdw_spi_rollback RESTRICT; }
step "s2_drop_fdw_trigger" { DROP FOREIGN DATA WRAPPER fdw_trigger RESTRICT; }
step "s2_drop_fdw_trigger_spi" { DROP FOREIGN DATA WRAPPER fdw_trigger_spi_func RESTRICT; }
step "s2_drop_fdw_inner" { DROP FOREIGN DATA WRAPPER fdw_inner RESTRICT; }
step "s2_drop_role" { DROP ROLE regress_dependency; }
step "s2_commit" { COMMIT; }
step "s2_rollback" { ROLLBACK; }

session "s3"

step "s3_revoke_role" { REVOKE role_fdw FROM role_user; }
step "s3_revoke_role_inner" { REVOKE role_fdw_inner FROM role_user; }
step "s3_revoke_both_roles" { REVOKE role_fdw, role_fdw_inner FROM role_user; }

# create function - drop schema
permutation "s1_begin" "s1_create_function_in_schema" "s2_drop_schema" "s1_commit"
permutation "s2_begin" "s2_drop_schema" "s1_create_function_in_schema" "s2_commit"

# alter function - drop schema
permutation "s1_begin" "s1_alter_function_schema" "s2_drop_alterschema" "s1_commit"
permutation "s2_begin" "s2_drop_alterschema" "s1_alter_function_schema" "s2_commit"

# create function - drop argtype
permutation "s1_begin" "s1_create_function_with_argtype" "s2_drop_foo_type" "s1_commit"
permutation "s2_begin" "s2_drop_foo_type" "s1_create_function_with_argtype" "s2_commit"

# create function - drop rettype
permutation "s1_begin" "s1_create_function_with_rettype" "s2_drop_foo_rettype" "s1_commit"
permutation "s2_begin" "s2_drop_foo_rettype" "s1_create_function_with_rettype" "s2_commit"

# create function - drop function used in its body
permutation "s1_begin" "s1_create_function_with_function" "s2_drop_function_f" "s1_commit"
permutation "s2_begin" "s2_drop_function_f" "s1_create_function_with_function" "s2_commit"

# create domain over domain - drop the base domain
permutation "s1_begin" "s1_create_domain_with_domain" "s2_drop_domain_id" "s1_commit"
permutation "s2_begin" "s2_drop_domain_id" "s1_create_domain_with_domain" "s2_commit"

# create table - drop type used in column
permutation "s1_begin" "s1_create_table_with_type" "s2_drop_footab_type" "s1_commit"
permutation "s2_begin" "s2_drop_footab_type" "s1_create_table_with_type" "s2_commit"

# create server - drop foreign data wrapper
permutation "s1_begin" "s1_create_server_with_fdw_wrapper" "s2_drop_fdw_wrapper" "s1_commit"
permutation "s2_begin" "s2_drop_fdw_wrapper" "s1_create_server_with_fdw_wrapper" "s2_commit"

# create function - drop owner role
permutation "s1_begin" "s1_alter_function_owner" "s2_drop_role" "s1_commit"

# XXX: This permutation is disabled because the error message, "role
# <OID> was concurrently dropped", contains an OID that is not stable.
#
# permutation "s2_begin" "s2_drop_role" "s1_alter_function_owner" "s2_commit"

# Role membership TOCTOU: permission via role revoked during lock wait.
permutation "s2_begin" "s2_drop_fdw_wrapper" "s1_create_server_as_role_user" "s3_revoke_role" "s2_rollback"

# Role membership TOCTOU with SPI DDL in FDW validator.
permutation "s2_begin" "s2_drop_fdw_spi_func" "s1_create_server_spi_func" "s3_revoke_role" "s2_rollback"

# Same as above but the validator's SPI DDL fails and rolls back its
# subtransaction.
permutation "s2_begin" "s2_drop_fdw_spi_rollback" "s1_create_server_spi_rollback" "s3_revoke_role" "s2_rollback"

# Role membership TOCTOU with DDL triggered from DML: a trigger function does
# DDL via SPI during INSERT.
permutation "s2_begin" "s2_drop_fdw_trigger" "s1_insert_trigger_ddl" "s3_revoke_role" "s2_rollback"

# Same as above but the DDL inside the trigger uses an FDW with a SPI validator,
# combining trigger-initiated DDL with nested SPI.
permutation "s2_begin" "s2_drop_fdw_trigger_spi" "s1_insert_trigger_spi_ddl" "s3_revoke_role" "s2_rollback"

# TOCTOU on the nested SPI DDL itself: the validator creates a server using
# fdw_inner, session 2 blocks that by dropping fdw_inner, and the REVOKE
# removes access to fdw_inner.
permutation "s2_begin" "s2_drop_fdw_inner" "s1_create_server_nested_toctou" "s3_revoke_both_roles" "s2_rollback"

# Rolled-back nested DDL should not cause the outer to fail: the validator
# tries to create a server using fdw_inner (via role_fdw_inner) but rolls back.
# The REVOKE removes role_fdw_inner, but the outer CREATE SERVER only needs
# role_fdw (for fdw_outer_rollback), so it should succeed.
permutation "s2_begin" "s2_drop_fdw_inner" "s1_create_server_nested_rollback" "s3_revoke_role_inner" "s2_rollback"
