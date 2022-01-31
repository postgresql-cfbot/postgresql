/* contrib/postgres_fdw/postgres_fdw--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION postgres_fdw UPDATE TO '1.2'" to load this file. \quit

GRANT SET VALUE ON "postgres_fdw.application_name" TO public;
