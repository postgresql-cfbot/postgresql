CREATE EXTENSION snowflake;

CREATE SEQUENCE snowflake_seq USING snowflake;

SET snowflake.machine_id = 2000; -- error
SET snowflake.machine_id = 4; -- ok
SELECT machine, counter FROM snowflake_get(nextval('snowflake_seq'));
SELECT machine, counter FROM snowflake_get(lastval());
SELECT machine, counter FROM snowflake_get(nextval('snowflake_seq'));
SELECT machine, counter FROM snowflake_get(currval('snowflake_seq'));

-- Sequence relation exists, is unlogged and remains unlogged.
SELECT * FROM snowflake_seq;
ALTER SEQUENCE snowflake_seq SET LOGGED;
SELECT relpersistence FROM pg_class where relname = 'snowflake_seq';

ALTER SEQUENCE snowflake_seq RESTART;
SELECT * FROM snowflake_seq;

-- Identity column, where cache affects value.
SET default_sequence_access_method = 'snowflake';
CREATE TABLE snowflake_tab (a int GENERATED ALWAYS AS IDENTITY, b int);
INSERT INTO snowflake_tab VALUES (DEFAULT, generate_series(1, 10));
SELECT data.machine, data.counter
  FROM snowflake_tab, LATERAL snowflake_get(a) AS data;
DROP TABLE snowflake_tab;

DROP SEQUENCE snowflake_seq;
DROP EXTENSION snowflake;
