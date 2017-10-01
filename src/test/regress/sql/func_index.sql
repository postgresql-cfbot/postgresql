create table keyvalue(id integer primary key, info jsonb);
create index nameindex on keyvalue((info->>'name')) with (projection=false);
set client_min_messages=debug1;
insert into keyvalue values (1, '{"name": "john", "data": "some data"}');
update keyvalue set info='{"name": "john", "data": "some other data"}' where id=1;
set client_min_messages=notice;
drop table keyvalue;

create table keyvalue(id integer primary key, info jsonb);
create index nameindex on keyvalue((info->>'name')) with (projection=true);
set client_min_messages=debug1;
insert into keyvalue values (1, '{"name": "john", "data": "some data"}');
update keyvalue set info='{"name": "john", "data": "some other data"}' where id=1;
update keyvalue set info='{"name": "smith", "data": "some other data"}' where id=1;
update keyvalue set info='{"name": "smith", "data": "some more data"}' where id=1;
set client_min_messages=notice;
drop table keyvalue;

create table keyvalue(id integer primary key, info jsonb);
create index nameindex on keyvalue((info->>'name'));
set client_min_messages=debug1;
insert into keyvalue values (1, '{"name": "john", "data": "some data"}');
update keyvalue set info='{"name": "john", "data": "some other data"}' where id=1;
update keyvalue set info='{"name": "smith", "data": "some other data"}' where id=1;
update keyvalue set info='{"name": "smith", "data": "some more data"}' where id=1;
set client_min_messages=notice;
drop table keyvalue;


