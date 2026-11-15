\echo Use "ALTER EXTENSION fasttrun UPDATE TO '2.1'" to load this file. \quit

-- Restore the canonical C API and retain the extension-owned legacy alias.
--
-- Cleanup. Only a schema-qualified, single-call PL/pgSQL forwarding wrapper
-- is removable. Its text signature and C alias must use the extension schema.
-- The alias must already belong to fasttrun and use this library and symbol.
-- Both functions must retain default ACLs, ownership and invoker security.
--
-- Conflicts. Other external text functions abort the upgrade unchanged.
-- DROP without CASCADE also protects objects that depend on the wrapper.
DO $cleanup_wrapper$
DECLARE
    _extension oid;
    _owner oid;
    _namespace oid;
    _schema text;
    _wrapper pg_catalog.pg_proc;
    _alias pg_catalog.pg_proc;
    _call text;
    _is_legacy_alias boolean;
BEGIN
    SELECT e.oid, e.extowner, e.extnamespace, n.nspname
      INTO STRICT _extension, _owner, _namespace, _schema
      FROM pg_catalog.pg_extension e
      JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
     WHERE e.extname = 'fasttrun';

    SELECT p.* INTO _wrapper
      FROM pg_catalog.pg_proc p
     WHERE p.pronamespace = _namespace
       AND p.proname = 'fasttruncate' AND p.prokind = 'f'
       AND p.pronargs = 1
       AND p.proargtypes[0] = 'pg_catalog.text'::pg_catalog.regtype;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    IF EXISTS (SELECT FROM pg_catalog.pg_depend d
                WHERE d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
                  AND d.objid = _wrapper.oid AND d.objsubid = 0
                  AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
                  AND d.refobjid = _extension AND d.deptype = 'e') THEN
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM pg_catalog.pg_proc a
        JOIN pg_catalog.pg_language l ON l.oid = a.prolang
        JOIN pg_catalog.pg_depend d ON d.objid = a.oid
         WHERE a.pronamespace = _namespace
           AND a.proname = 'fasttruncate_c' AND a.prokind = 'f'
           AND a.pronargs = 1
           AND a.proargtypes[0] = 'pg_catalog.text'::pg_catalog.regtype
           AND a.prorettype = 'pg_catalog.void'::pg_catalog.regtype
           AND NOT a.proretset AND l.lanname = 'c'
           AND a.probin = 'MODULE_PATHNAME' AND a.prosrc = 'fasttruncate'
           AND d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
           AND d.objsubid = 0
           AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
           AND d.refobjid = _extension AND d.deptype = 'e'
    ) INTO _is_legacy_alias;

    IF _wrapper.prolang = (SELECT oid FROM pg_catalog.pg_language
                           WHERE lanname = 'plpgsql')
       AND _wrapper.prorettype = 'pg_catalog.void'::pg_catalog.regtype
       AND NOT _wrapper.proretset THEN
        _call := (pg_catalog.regexp_match(_wrapper.prosrc,
            '^[[:space:]]*BEGIN[[:space:]]+PERFORM[[:space:]]+(.+);[[:space:]]*END[[:space:]]*;[[:space:]]*$',
            'i'))[1];
    END IF;

    IF _is_legacy_alias
       AND _call IN (
           pg_catalog.format('%I.fasttruncate_c($1)', _schema),
           pg_catalog.format('%I.fasttruncate_c(%s)', _schema,
               COALESCE(pg_catalog.quote_ident(_wrapper.proargnames[1]), '$1')))
       AND NOT EXISTS (SELECT FROM pg_catalog.pg_depend d
                        WHERE d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
                          AND d.objid = _wrapper.oid AND d.objsubid = 0
                          AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
                          AND d.deptype = 'e') THEN
        SELECT p.* INTO STRICT _alias FROM pg_catalog.pg_proc p
         WHERE p.pronamespace = _namespace AND p.proname = 'fasttruncate_c'
           AND p.pronargs = 1 AND p.proargtypes[0] = 'pg_catalog.text'::pg_catalog.regtype;
        IF _wrapper.proowner <> _owner OR _alias.proowner <> _owner
           OR _wrapper.prosecdef OR _alias.prosecdef
           OR _wrapper.proconfig IS NOT NULL OR _alias.proconfig IS NOT NULL
           OR _wrapper.proleakproof OR _alias.proleakproof
           OR COALESCE(_wrapper.proacl, pg_catalog.acldefault('f', _owner))
                <> pg_catalog.acldefault('f', _owner)
           OR COALESCE(_alias.proacl, pg_catalog.acldefault('f', _owner))
                <> pg_catalog.acldefault('f', _owner) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'fasttrun upgrade cannot preserve legacy wrapper security automatically',
                DETAIL = 'The wrapper and C alias have nondefault ACLs, owners, security attributes or function settings. Replacing two permission checks with one can change effective access.',
                HINT = 'Migrate the wrapper explicitly in a transaction, preserving the intended owner, effective EXECUTE permissions and function settings, then run ALTER EXTENSION fasttrun UPDATE. Do not grant PUBLIC access just to bypass this check.';
        END IF;
        EXECUTE pg_catalog.format('DROP FUNCTION %I.fasttruncate(pg_catalog.text)',
                                  _schema);
    ELSE
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = pg_catalog.format(
                'fasttrun upgrade cannot replace independent function %I.fasttruncate(text)',
                _schema),
            DETAIL = 'The function is not the recognized legacy wrapper over the extension-owned C alias.',
            HINT = 'Resolve the function name conflict before retrying ALTER EXTENSION fasttrun UPDATE.';
    END IF;
END
$cleanup_wrapper$;

-- CREATE OR REPLACE retains the OID and grants of an existing C function.
CREATE OR REPLACE FUNCTION fasttruncate(text)
RETURNS void AS 'MODULE_PATHNAME', 'fasttruncate'
LANGUAGE C RETURNS NULL ON NULL INPUT VOLATILE;

-- Existing callers and dependencies keep the renamed extension-owned C alias.
-- Fresh installs omit the alias because only upgrade clients need it.
CREATE OR REPLACE FUNCTION fasttruncate_c(text)
RETURNS void AS 'MODULE_PATHNAME', 'fasttruncate'
LANGUAGE C RETURNS NULL ON NULL INPUT VOLATILE;

-- ----------------------------------------------------------------------
-- Шаг 4: новые в 2.1
-- ----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fasttrun_analyze(text)
RETURNS void AS 'MODULE_PATHNAME', 'fasttrun_analyze'
LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE FUNCTION fasttrun_relstats(IN  rel_name text,
                                             OUT relpages int,
                                             OUT reltuples real)
RETURNS record AS 'MODULE_PATHNAME', 'fasttrun_relstats'
LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE FUNCTION fasttrun_collect_stats(text)
RETURNS void AS 'MODULE_PATHNAME', 'fasttrun_collect_stats'
LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE FUNCTION fasttrun_inspect_stats(rel_name text)
RETURNS SETOF pg_catalog.pg_statistic
AS 'MODULE_PATHNAME', 'fasttrun_inspect_stats'
LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE FUNCTION fasttrun_hot_temp_tables(
    n int DEFAULT 100,
    OUT relname text,
    OUT create_count bigint,
    OUT last_create timestamptz)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'fasttrun_hot_temp_tables'
LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE FUNCTION fasttrun_prewarm()
RETURNS int
AS 'MODULE_PATHNAME', 'fasttrun_prewarm'
LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION fasttrun_reset_temp_stats()
RETURNS void
AS 'MODULE_PATHNAME', 'fasttrun_reset_temp_stats'
LANGUAGE C VOLATILE;
