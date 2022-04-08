setup
{
  CREATE SCHEMA testschema;
  CREATE USER toorph;
  GRANT ALL PRIVILEGES ON schema public TO toorph;
}

teardown
{
  DROP FUNCTION IF EXISTS functime();
  DROP FUNCTION IF EXISTS testschema.functime();
  DROP SCHEMA IF EXISTS testschema;
  REVOKE ALL PRIVILEGES ON schema public from toorph;
  DROP USER toorph;
}

session "s1"

step "s1_begin" { BEGIN; }
step "s1_create_function_in_schema" { CREATE OR REPLACE FUNCTION testschema.functime() RETURNS TIMESTAMP AS $$
DECLARE
   outTS TIMESTAMP;
BEGIN
   RETURN CURRENT_TIMESTAMP;
END;
$$ LANGUAGE plpgsql volatile;
 }
step "s1_create_function" { CREATE OR REPLACE FUNCTION functime() RETURNS TIMESTAMP AS $$
DECLARE
   outTS TIMESTAMP;
BEGIN
   RETURN CURRENT_TIMESTAMP;
END;
$$ LANGUAGE plpgsql volatile;
 }
step "s1_set_session_authorization" { set SESSION AUTHORIZATION toorph; }
step "s1_commit" { COMMIT; }

session "s2"

step "s2_begin" { BEGIN; }
step "s2_drop_schema" { drop schema testschema; }
step "s2_drop_owned_by_toorph" { drop owned by toorph; }
step "s2_commit" { COMMIT; }

permutation "s1_begin" "s1_create_function_in_schema" "s2_drop_schema" "s1_commit"
permutation "s2_begin" "s2_drop_schema" "s1_create_function_in_schema" "s2_commit"
permutation "s1_set_session_authorization" "s1_begin" "s1_create_function" "s2_drop_owned_by_toorph" "s1_commit"
