CREATE EXTENSION jsonb_plperlu CASCADE;
CREATE SCHEMA test;
alter extension jsonb_plperlu set schema test;
create function test.test(val jsonb) returns jsonb
language plperlu
transform for type jsonb
as $$
return val
$$;

select test.test('1'::jsonb);

drop extension plperlu cascade;
drop schema test cascade;
