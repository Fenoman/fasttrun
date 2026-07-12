\echo Use "ALTER EXTENSION fasttrun UPDATE TO '2.3.0'" to load this file. \quit

-- ----------------------------------------------------------------------
-- 2.2.0 -> 2.3.0: batch variant of fasttrun_analyze plus a pile of
-- C-only changes -- xact-callback overhead fix, lazy reinject memo,
-- the invalidate_threshold GUC, and the evict_temp_relid refactor.
-- The C bits need no SQL surface change.  We ALSO add
-- fasttrun_analyze_bulk here so backends upgrading from 2.2.0 pick the
-- new function up without dropping and recreating the extension.
-- ----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fasttrun_analyze_bulk(VARIADIC text[])
RETURNS void AS 'MODULE_PATHNAME', 'fasttrun_analyze_bulk'
LANGUAGE C VOLATILE;
