-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_metrics" to load this file. \quit

CREATE TYPE metric_type AS ENUM (
	'COUNTER'
);

CREATE FUNCTION counter_add(counter text, increment int8)
RETURNS int8
AS 'pg_metrics', 'pgmet_counter_add' LANGUAGE c STRICT;

CREATE FUNCTION metrics() RETURNS TABLE (
	metric_name text,
	metric_type metric_type,
	counter_value int8
)
AS 'pg_metrics', 'pgmet_metrics' LANGUAGE c STRICT;

CREATE FUNCTION metrics_stats(OUT max_metrics int4, OUT num_metrics int4) RETURNS RECORD
AS 'pg_metrics', 'pgmet_metrics_stats' LANGUAGE c STRICT;
