CREATE EXTENSION pg_buffercache;

select count(*) = (select setting
                    from pg_show_all_settings()
                    where name = 'shared_buffers')::bigint
from pg_buffercache;

