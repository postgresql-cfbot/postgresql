/* contrib/zson/zson--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION zson" to load this file. \quit

SELECT pg_catalog.pg_extension_config_dump('zson_dict', '');
