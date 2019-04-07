--
--  Test plpgsql_tracer extension
--

load 'plpgsql';
load 'plpgsql_tracer';

create or replace function fx()
returns integer
language plpgsql
AS $function$
declare
  x int = 0;
  pragma plpgsql_tracer(on);
  pragma ignore_me;
begin
  x := x + 1;
  x := x + 1;
  declare
    pragma plpgsql_tracer(off);
  begin
    -- hidden statemt
    declare pragma plpgsql_tracer; -- ignored, just warning
    begin
      x := x + 1;
    end;
  end;
  return x;
end;
$function$;

select fx();
