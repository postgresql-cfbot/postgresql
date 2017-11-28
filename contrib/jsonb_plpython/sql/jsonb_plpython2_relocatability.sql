create extension jsonb_plpython2u cascade;
create schema test;
alter extension jsonb_plpython2u set schema test;
create function test.test(val jsonb) returns jsonb
language plpython2u
transform for type jsonb
as $$
return val
$$;

select test.test('1'::jsonb);

drop schema test cascade;
