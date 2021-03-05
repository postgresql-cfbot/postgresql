/* contrib/hstore/hstore--1.8--1.9.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION hstore UPDATE TO '1.9'" to load this file. \quit

-- Remove @
DROP OPERATOR @ (hstore, hstore);
