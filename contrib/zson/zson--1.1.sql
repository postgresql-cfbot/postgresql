/* contrib/zson/zson--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION zson" to load this file. \quit

CREATE TYPE zson;

CREATE TABLE zson_dict (
    dict_id SERIAL NOT NULL,
    word_id INTEGER NOT NULL,
    word text NOT NULL,
    PRIMARY KEY(dict_id, word_id)
);

SELECT pg_catalog.pg_extension_config_dump('zson_dict', '');

CREATE FUNCTION zson_learn(
    tables_and_columns text[][],
    max_examples int default 10000,
    min_length int default 2,
    max_length int default 128,
    min_count int default 2)
    RETURNS text AS $$
DECLARE
    tabname text;
    colname text;
    query text := '';
    i int;
    next_dict_id int;
BEGIN
    IF cardinality(tables_and_columns) = 0 THEN
        RAISE NOTICE 'First argument should not be an empty array!';
        RETURN '';
    END IF;

    FOR i IN    array_lower(tables_and_columns, 1) ..
                array_upper(tables_and_columns, 1)
    LOOP
        tabname := tables_and_columns[i][1];
        colname := tables_and_columns[i][2];

        IF (tabname IS NULL) OR (colname IS NULL) THEN
            RAISE NOTICE 'Invalid list of tables and columns!';
            RETURN '';
        ELSIF position('"' in tabname) <> 0 THEN
            RAISE NOTICE 'Invalid table name %', tabname;
            RETURN '';
        ELSIF position('"' in colname) <> 0 THEN
            RAISE NOTICE 'Invalid column name %', tabname;
            RETURN '';
        ELSIF position('.' in tabname) <> 0 THEN
            tabname := quote_ident(split_part(tabname, '.', 1)) ||
                '.' || quote_ident(split_part(tabname, '.', 2));
        END IF;

        IF query <> '' THEN
            query := query || ' UNION ALL ';
        END IF;

        query := query || '( SELECT unnest(zson_extract_strings(' ||
            quote_ident(colname) || ')) AS t FROM ' || tabname || ' LIMIT ' ||
            max_examples || ')';

    END LOOP;

    SELECT coalesce(max(dict_id), -1) + 1 INTO next_dict_id FROM zson_dict;

    query := 'SELECT t FROM (SELECT t, count(*) AS sum FROM ( ' ||
        query || ' ) AS tt GROUP BY t) AS s WHERE length(t) >= ' ||
        min_length || ' AND length(t) <= ' || max_length ||
        ' AND sum >= ' || min_count || ' ORDER BY sum DESC LIMIT 65534';

    query := 'INSERT INTO zson_dict SELECT ' || next_dict_id ||
        ' AS dict_id, row_number() OVER () AS word_id, t AS word FROM ( ' ||
        query || ' ) AS top_words';

    EXECUTE query;

    RETURN 'Done! Run " SELECT * FROM zson_dict WHERE dict_id = ' ||
        next_dict_id || '; " to see the dictionary.';
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION zson_extract_strings(x jsonb)
    RETURNS text[] AS $$
DECLARE
    jtype text;
    jitem jsonb;
BEGIN
    jtype := jsonb_typeof(x);
    IF jtype = 'object' THEN
        RETURN array(SELECT unnest(z) FROM (
                SELECT array(SELECT jsonb_object_keys(x)) AS z
            UNION ALL (
                SELECT zson_extract_strings(x -> k) AS z FROM (
                    SELECT jsonb_object_keys(x) AS k
                ) AS kk
            )
        ) AS zz);
    ELSIF jtype = 'array' THEN
       RETURN ARRAY(SELECT unnest(zson_extract_strings(t)) FROM
            (SELECT jsonb_array_elements(x) AS t) AS tt);
    ELSIF jtype = 'string' THEN
        RETURN array[ x #>> array[] :: text[] ];
    ELSE -- 'number', 'boolean', 'bool'
        RETURN array[] :: text[];
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION zson_in(cstring)
    RETURNS zson
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION zson_out(zson)
    RETURNS cstring
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE TYPE zson (
    INTERNALLENGTH = -1,
    INPUT = zson_in,
    OUTPUT = zson_out,
    STORAGE = extended -- try to compress
);

CREATE FUNCTION jsonb_to_zson(jsonb)
    RETURNS zson
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION zson_to_jsonb(zson)
    RETURNS jsonb
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE CAST (jsonb AS zson) WITH FUNCTION jsonb_to_zson(jsonb) AS ASSIGNMENT;
CREATE CAST (zson AS jsonb) WITH FUNCTION zson_to_jsonb(zson) AS IMPLICIT;

CREATE FUNCTION zson_info(zson)
    RETURNS cstring
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;
