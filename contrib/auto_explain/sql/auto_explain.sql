LOAD 'auto_explain';

SET auto_explain.log_min_duration = 0;
SET auto_explain.log_duration = off;
SET auto_explain.log_level = NOTICE;
SET auto_explain.log_timing = off;
SET auto_explain.log_costs = off;

-- Should log
SELECT NULL FROM pg_catalog.pg_class WHERE relname = 'pg_class';

SET auto_explain.log_verbose = on;
SELECT NULL FROM pg_catalog.pg_class WHERE relname = 'pg_class';

SET auto_explain.log_format = json;
SELECT NULL FROM pg_catalog.pg_class WHERE relname = 'pg_class';

-- Shouldn't log due to too high loglevel
SET auto_explain.log_level = DEBUG5;
SELECT NULL FROM pg_catalog.pg_class WHERE relname = 'pg_class';

-- Shouldn't log due to query being too fast
SET auto_explain.log_level = NOTICE;
SET auto_explain.log_min_duration = 1000;
SELECT NULL FROM pg_catalog.pg_class WHERE relname = 'pg_class';

