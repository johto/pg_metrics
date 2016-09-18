-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_metrics" to load this file. \quit

CREATE FUNCTION counter_add(counter name, increment int8)
	RETURNS int8
	AS 'pg_metrics', 'pgmet_counter_add' LANGUAGE c STRICT;
