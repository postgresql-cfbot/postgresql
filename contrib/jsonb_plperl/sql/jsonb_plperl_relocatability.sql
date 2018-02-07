CREATE EXTENSION jsonb_plperl CASCADE;
CREATE SCHEMA test;
alter extension jsonb_plperl set schema test;
create function test.test(val jsonb) returns jsonb
language plperl
transform for type jsonb
as $$
return val
$$;

select test.test('1'::jsonb);

drop extension plperl cascade;
drop schema test cascade;
