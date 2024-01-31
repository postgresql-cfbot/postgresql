--
-- COPY TO JSON
--

CREATE EXTENSION pg_copy_json;

-- test copying in JSON format with various styles
-- of embedded line ending characters

create temp table copytest (
	style	text,
	test 	text,
	filler	int);

insert into copytest values('DOS',E'abc\r\ndef',1);
insert into copytest values('Unix',E'abc\ndef',2);
insert into copytest values('Mac',E'abc\rdef',3);
insert into copytest values(E'esc\\ape',E'a\\r\\\r\\\n\\nb',4);

copy copytest to stdout with (format 'json');

-- pg_copy_json do not support COPY FROM
copy copytest from stdout with (format 'json');

-- test copying in JSON format with various styles
-- of embedded escaped characters

create temp table copyjsontest (
    id bigserial,
    f1 text,
    f2 timestamptz);

insert into copyjsontest
  select g.i,
         CASE WHEN g.i % 2 = 0 THEN
           'line with '' in it: ' || g.i::text
         ELSE
           'line with " in it: ' || g.i::text
         END,
         'Mon Feb 10 17:32:01 1997 PST'
  from generate_series(1,5) as g(i);

insert into copyjsontest (f1) values
(E'aaa\"bbb'::text),
(E'aaa\\bbb'::text),
(E'aaa\/bbb'::text),
(E'aaa\bbbb'::text),
(E'aaa\fbbb'::text),
(E'aaa\nbbb'::text),
(E'aaa\rbbb'::text),
(E'aaa\tbbb'::text);

copy copyjsontest to stdout with (format 'json');

-- test force array

copy copytest to stdout (format 'json', force_array);
copy copytest to stdout (format 'json', force_array true);
copy copytest to stdout (format 'json', force_array false);
