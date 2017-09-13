/* contrib/cube/cube--1.2--1.3.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION btree_gist UPDATE TO '1.5'" to load this file. \quit


ALTER OPERATOR FAMILY gist_cube_ops USING gist ADD
	FUNCTION	9 (cube, cube) g_cube_decompress (internal) ;
