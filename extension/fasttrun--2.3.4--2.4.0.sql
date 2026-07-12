\echo Use "ALTER EXTENSION fasttrun UPDATE TO '2.4.0'" to load this file. \quit

CREATE OR REPLACE FUNCTION fasttrun_cache_stats(
    OUT analyze_entries bigint,
    OUT column_stats_relid_entries bigint,
    OUT column_stats_entries bigint,
    OUT analyze_bytes bigint,
    OUT column_stats_bytes bigint,
    OUT total_bytes bigint)
RETURNS record AS 'MODULE_PATHNAME', 'fasttrun_cache_stats'
LANGUAGE C VOLATILE;
