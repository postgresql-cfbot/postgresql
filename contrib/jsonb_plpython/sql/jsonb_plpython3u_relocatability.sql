create extension jsonb_plpython3u cascade;
create schema test;
alter extension jsonb_plpython3u set schema test;
create function test.test(val jsonb) returns jsonb
language plpython3u
transform for type jsonb
as $$
return val
$$;

select test.test('1'::jsonb);

drop schema test cascade;
drop extension plpython3u cascade;
