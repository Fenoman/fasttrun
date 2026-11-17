-- Replace simple ANALYZE temp_* statements in PL/pgSQL function bodies.
--
-- Usage:
--   1. Run STEP 1 and inspect both the original and proposed bodies.
--   2. Uncomment STEP 2 and run it as a superuser.
--   3. Inspect APPLIED/SKIP results, then COMMIT or ROLLBACK.
--
-- Safety:
--   Bodies containing single quotes, dollar signs, or comment delimiters are
--   skipped in full. Review their function DDL manually. Double-quoted
--   identifiers are supported unless they contain a semicolon, a backslash,
--   or are unclosed. Replacement calls are schema-qualified with the schema
--   of extension fasttrun (or public when the extension is not yet installed).
--
-- Matching:
--   A statement starts after BEGIN/THEN/ELSE/LOOP, ';', or the start of the body.
--   Its final ';' is a lookahead, so adjacent ANALYZE statements all match.
--   Identifier quotes remain inside the fasttrun_analyze argument. Bodies with
--   a remaining ANALYZE temp_* candidate are skipped instead of partly changed.

-- STEP 1: DRY RUN
WITH ext AS MATERIALIZED (
    SELECT replace(
        coalesce(
            (SELECT pg_catalog.quote_ident(n.nspname)
               FROM pg_catalog.pg_extension e
               JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
              WHERE e.extname = 'fasttrun'),
            'public'
        ),
        E'\\', E'\\\\'
    ) AS schema_repl
), candidates AS MATERIALIZED (
    SELECT p.oid AS function_oid,
           p.oid::regprocedure::text AS function_name,
           p.prosrc AS original_body,
           ext.schema_repl,
           CASE
               WHEN strpos(p.prosrc, chr(39)) > 0 THEN 'single quote'
               WHEN strpos(p.prosrc, '$') > 0 THEN 'dollar sign'
               WHEN strpos(p.prosrc, '--') > 0 THEN 'line comment'
               WHEN strpos(p.prosrc, '/*') > 0 THEN 'block comment'
               WHEN EXISTS (
                   SELECT 1
                   FROM regexp_matches(p.prosrc, '"(?:[^"]|"")*"', 'g') AS q(parts)
                   WHERE strpos(q.parts[1], ';') > 0
               ) THEN 'semicolon in quoted identifier'
               WHEN EXISTS (
                   SELECT 1
                   FROM regexp_matches(p.prosrc, '"(?:[^"]|"")*"', 'g') AS q(parts)
                   WHERE strpos(q.parts[1], chr(92)) > 0
               ) THEN 'backslash in quoted identifier'
               WHEN regexp_replace(p.prosrc, '"(?:[^"]|"")*"', '', 'g') ~ '"'
                   THEN 'unclosed quoted identifier'
           END AS skip_reason
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    JOIN pg_language l ON l.oid = p.prolang
    CROSS JOIN ext
    WHERE l.lanname = 'plpgsql'
      AND p.prosrc ~* E'\\mANALYZE\\s+(?:\\([^)]*\\)\\s*)?(?:[[:alnum:]_"]+\\s*\\.\\s*)?"?temp[ _]'
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
), proposals AS (
    SELECT *, CASE WHEN skip_reason IS NULL THEN regexp_replace(
        original_body,
        E'(^|;|\\m(?:BEGIN|THEN|ELSE|LOOP)\\M)(\\s*)ANALYZE\\s+("temp[ _](?:[^"]|"")+"|temp_\\w+)(?=\\s*;)',
        E'\\1\\2PERFORM ' || schema_repl || E'.fasttrun_analyze(''\\3'')',
        'gi'
    ) ELSE original_body END AS proposed_body
    FROM candidates
), assessed AS (
    SELECT *, coalesce(skip_reason,
        CASE WHEN proposed_body ~* E'\\mANALYZE\\s+(?:\\([^)]*\\)\\s*)?(?:[[:alnum:]_"]+\\s*\\.\\s*)?"?temp[ _]'
             THEN 'unsupported ANALYZE statement position or identifier' END
    ) AS reason
    FROM proposals
)
SELECT function_oid, function_name,
       CASE WHEN reason IS NULL THEN 'READY' ELSE 'SKIP' END AS status,
       reason, original_body,
       CASE WHEN reason IS NULL THEN proposed_body ELSE original_body END AS proposed_body
FROM assessed
ORDER BY function_name;

-- STEP 2: APPLY (uncomment this block)
-- APPLY_BEGIN
-- BEGIN;
--
-- WITH ext AS MATERIALIZED (
--     SELECT replace(
--         coalesce(
--             (SELECT pg_catalog.quote_ident(n.nspname)
--                FROM pg_catalog.pg_extension e
--                JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
--               WHERE e.extname = 'fasttrun'),
--             'public'
--         ),
--         E'\\', E'\\\\'
--     ) AS schema_repl
-- ), candidates AS MATERIALIZED (
--     SELECT p.oid AS function_oid,
--            p.oid::regprocedure::text AS function_name,
--            p.prosrc AS original_body,
--            ext.schema_repl,
--            CASE
--                WHEN strpos(p.prosrc, chr(39)) > 0 THEN 'single quote'
--                WHEN strpos(p.prosrc, '$') > 0 THEN 'dollar sign'
--                WHEN strpos(p.prosrc, '--') > 0 THEN 'line comment'
--                WHEN strpos(p.prosrc, '/*') > 0 THEN 'block comment'
--                WHEN EXISTS (
--                    SELECT 1
--                    FROM regexp_matches(p.prosrc, '"(?:[^"]|"")*"', 'g') AS q(parts)
--                    WHERE strpos(q.parts[1], ';') > 0
--                ) THEN 'semicolon in quoted identifier'
--                WHEN EXISTS (
--                    SELECT 1
--                    FROM regexp_matches(p.prosrc, '"(?:[^"]|"")*"', 'g') AS q(parts)
--                    WHERE strpos(q.parts[1], chr(92)) > 0
--                ) THEN 'backslash in quoted identifier'
--                WHEN regexp_replace(p.prosrc, '"(?:[^"]|"")*"', '', 'g') ~ '"'
--                    THEN 'unclosed quoted identifier'
--            END AS skip_reason
--     FROM pg_proc p
--     JOIN pg_namespace n ON n.oid = p.pronamespace
--     JOIN pg_language l ON l.oid = p.prolang
--     CROSS JOIN ext
--     WHERE l.lanname = 'plpgsql'
--       AND p.prosrc ~* E'\\mANALYZE\\s+(?:\\([^)]*\\)\\s*)?(?:[[:alnum:]_"]+\\s*\\.\\s*)?"?temp[ _]'
--       AND n.nspname NOT IN ('pg_catalog', 'information_schema')
-- ), proposals AS (
--     SELECT *, CASE WHEN skip_reason IS NULL THEN regexp_replace(
--         original_body,
--         E'(^|;|\\m(?:BEGIN|THEN|ELSE|LOOP)\\M)(\\s*)ANALYZE\\s+("temp[ _](?:[^"]|"")+"|temp_\\w+)(?=\\s*;)',
--         E'\\1\\2PERFORM ' || schema_repl || E'.fasttrun_analyze(''\\3'')',
--         'gi'
--     ) ELSE original_body END AS proposed_body
--     FROM candidates
-- ), assessed AS (
--     SELECT *, coalesce(skip_reason,
--         CASE WHEN proposed_body ~* E'\\mANALYZE\\s+(?:\\([^)]*\\)\\s*)?(?:[[:alnum:]_"]+\\s*\\.\\s*)?"?temp[ _]'
--              THEN 'unsupported ANALYZE statement position or identifier' END
--     ) AS reason
--     FROM proposals
-- ), updated AS (
--     UPDATE pg_proc p
--     SET prosrc = a.proposed_body
--     FROM assessed a
--     WHERE p.oid = a.function_oid
--       AND p.prosrc = a.original_body
--       AND a.reason IS NULL
--       AND a.proposed_body != a.original_body
--     RETURNING p.oid AS function_oid
-- )
-- SELECT a.function_oid, a.function_name,
--        CASE WHEN u.function_oid IS NOT NULL THEN 'APPLIED' ELSE 'SKIP' END AS status,
--        coalesce(a.reason,
--            CASE WHEN u.function_oid IS NULL THEN 'source changed during apply. Re-run preview' END
--        ) AS reason
-- FROM assessed a
-- LEFT JOIN updated u USING (function_oid)
-- ORDER BY a.function_name;
-- APPLY_END
--
-- COMMIT;
-- ROLLBACK;
