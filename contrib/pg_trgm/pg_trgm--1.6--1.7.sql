/* contrib/pg_trgm/pg_trgm--1.6--1.7.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_trgm UPDATE TO '1.7'" to load this file. \quit

GRANT SET VALUE ON "pg_trgm.similarity_threshold" TO public;
GRANT SET VALUE ON "pg_trgm.word_similarity_threshold" TO public;
GRANT SET VALUE ON "pg_trgm.strict_word_similarity_threshold" TO public;
