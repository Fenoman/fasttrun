\echo Use "ALTER EXTENSION fasttrun UPDATE TO '2.1'" to load this file. \quit

-- Restore the canonical C API and retain the extension-owned legacy alias.
--
-- Cleanup. Only a schema-qualified, single-call PL/pgSQL forwarding wrapper
-- is removable. Its text signature and C alias must use the extension schema.
-- The alias must already belong to fasttrun and use this library and symbol.
-- Both functions must retain default ACLs, expected ownership and invoker security.
--
-- Conflicts. Other external text functions abort the upgrade unchanged.
-- DROP without CASCADE also protects objects that depend on the wrapper.
DO $cleanup_wrapper$
DECLARE
    _extension oid;
    _ext_owner oid;
    _ext_is_super boolean;
    _namespace oid;
    _schema text;
    _wrapper pg_catalog.pg_proc;
    _alias pg_catalog.pg_proc;
    _call text;
    _is_legacy_alias boolean;
BEGIN
    PERFORM pg_catalog.set_config('pg_fasttrun_mig.sync_from_alias', 'false', true);

    SELECT e.oid, e.extowner, r.rolsuper, e.extnamespace, n.nspname
      INTO STRICT _extension, _ext_owner, _ext_is_super, _namespace, _schema
      FROM pg_catalog.pg_extension e
      JOIN pg_catalog.pg_roles r ON r.oid = e.extowner
      JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
     WHERE e.extname = 'fasttrun';

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

    SELECT p.* INTO _wrapper
      FROM pg_catalog.pg_proc p
     WHERE p.pronamespace = _namespace
       AND p.proname = 'fasttruncate' AND p.prokind = 'f'
       AND p.pronargs = 1
       AND p.proargtypes[0] = 'pg_catalog.text'::pg_catalog.regtype;

    IF NOT FOUND THEN
        IF _is_legacy_alias THEN
            SELECT p.* INTO STRICT _alias FROM pg_catalog.pg_proc p
             WHERE p.pronamespace = _namespace AND p.proname = 'fasttruncate_c'
               AND p.pronargs = 1
               AND p.proargtypes[0] = 'pg_catalog.text'::pg_catalog.regtype;
            IF _alias.prosecdef OR _alias.proconfig IS NOT NULL
               OR _alias.proleakproof THEN
                RAISE EXCEPTION USING
                    ERRCODE = '55000',
                    MESSAGE = 'fasttrun upgrade cannot preserve legacy C alias security automatically',
                    DETAIL = 'The extension-owned C alias fasttruncate_c(text) has nondefault security attributes or function settings.',
                    HINT = 'Migrate fasttruncate_c(text) explicitly in a transaction, preserving the intended owner, effective EXECUTE permissions and function settings, then run ALTER EXTENSION fasttrun UPDATE. Do not grant PUBLIC access just to bypass this check.';
            END IF;
            PERFORM pg_catalog.set_config('pg_fasttrun_mig.sync_from_alias', 'true', true);
        END IF;
        RETURN;
    END IF;

    IF EXISTS (SELECT FROM pg_catalog.pg_depend d
                WHERE d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
                  AND d.objid = _wrapper.oid AND d.objsubid = 0
                  AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
                  AND d.refobjid = _extension AND d.deptype = 'e') THEN
        RETURN;
    END IF;

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
        IF _alias.proowner NOT IN (
               10::pg_catalog.oid,
               CASE WHEN _ext_is_super THEN _ext_owner ELSE 10::pg_catalog.oid END
           )
           OR _wrapper.proowner <> _alias.proowner
           OR _wrapper.prosecdef OR _alias.prosecdef
           OR _wrapper.proconfig IS NOT NULL OR _alias.proconfig IS NOT NULL
           OR _wrapper.proleakproof OR _alias.proleakproof
           OR COALESCE(_wrapper.proacl, pg_catalog.acldefault('f', _wrapper.proowner))
                <> pg_catalog.acldefault('f', _wrapper.proowner)
           OR COALESCE(_alias.proacl, pg_catalog.acldefault('f', _alias.proowner))
                <> pg_catalog.acldefault('f', _alias.proowner) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'fasttrun upgrade cannot preserve legacy wrapper security automatically',
                DETAIL = 'The wrapper and C alias have nondefault ACLs, owners, security attributes or function settings. Replacing two permission checks with one can change effective access.',
                HINT = 'Migrate the wrapper explicitly in a transaction, preserving the intended owner, effective EXECUTE permissions and function settings, then run ALTER EXTENSION fasttrun UPDATE. Do not grant PUBLIC access just to bypass this check.';
        END IF;
        EXECUTE pg_catalog.format('DROP FUNCTION %I.fasttruncate(pg_catalog.text)',
                                  _schema);
        PERFORM pg_catalog.set_config('pg_fasttrun_mig.sync_from_alias', 'true', true);
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

-- If fasttruncate(text) was absent or replaced from a recognized legacy
-- wrapper, preserve the owner and exact ACL of the extension-owned C alias so
-- recreating the canonical name neither alters ownership nor re-opens PUBLIC.
-- Do not create fasttruncate_c(text) when it was not already present: when it
-- existed as the renamed 2.0 function it already belongs to the extension.
DO $sync_canonical_acl$
DECLARE
    _extension oid;
    _namespace oid;
    _schema text;
    _alias pg_catalog.pg_proc;
    _canon pg_catalog.pg_proc;
    _alias_owner oid;
    _alias_owner_name text;
    _exec_role oid;
    _helper_created boolean := false;
    _stmt text;
    _applied integer[] := '{}';
    _progressed boolean;
    _grant record;
BEGIN
    IF pg_catalog.current_setting('pg_fasttrun_mig.sync_from_alias', true)
       IS DISTINCT FROM 'true' THEN
        RETURN;
    END IF;
    PERFORM pg_catalog.set_config('pg_fasttrun_mig.sync_from_alias', 'false', true);

    SELECT e.oid, e.extnamespace, n.nspname
      INTO STRICT _extension, _namespace, _schema
      FROM pg_catalog.pg_extension e
      JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
     WHERE e.extname = 'fasttrun';

    SELECT a.* INTO _alias
      FROM pg_catalog.pg_proc a
      JOIN pg_catalog.pg_language l ON l.oid = a.prolang
      JOIN pg_catalog.pg_depend d ON d.objid = a.oid
     WHERE a.pronamespace = _namespace
       AND a.proname = 'fasttruncate_c' AND a.prokind = 'f'
       AND a.pronargs = 1
       AND a.proargtypes[0] = 'pg_catalog.text'::pg_catalog.regtype
       AND l.lanname = 'c'
       AND d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
       AND d.objsubid = 0
       AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
       AND d.refobjid = _extension AND d.deptype = 'e';

    IF NOT FOUND THEN
        RETURN;
    END IF;

    SELECT p.* INTO STRICT _canon
      FROM pg_catalog.pg_proc p
     WHERE p.pronamespace = _namespace
       AND p.proname = 'fasttruncate' AND p.prokind = 'f'
       AND p.pronargs = 1
       AND p.proargtypes[0] = 'pg_catalog.text'::pg_catalog.regtype;

    _alias_owner := _alias.proowner;
    _alias_owner_name := pg_catalog.pg_get_userbyid(_alias_owner);
    _exec_role := CURRENT_USER::pg_catalog.regrole::pg_catalog.oid;

    IF _canon.proowner <> _alias_owner THEN
        EXECUTE pg_catalog.format(
            'ALTER FUNCTION %I.fasttruncate(pg_catalog.text) OWNER TO %I',
            _schema, _alias_owner_name);
        SELECT p.* INTO STRICT _canon
          FROM pg_catalog.pg_proc p
         WHERE p.oid = _canon.oid;
    END IF;

    IF (
        SELECT ARRAY(
            SELECT pg_catalog.format('%s/%s:%s:%s',
                       a.grantor, a.grantee, a.privilege_type, a.is_grantable)
              FROM pg_catalog.aclexplode(
                       COALESCE(_canon.proacl, pg_catalog.acldefault('f', _canon.proowner))
                   ) AS a
             ORDER BY 1
        )
    ) IS NOT DISTINCT FROM (
        SELECT ARRAY(
            SELECT pg_catalog.format('%s/%s:%s:%s',
                       a.grantor, a.grantee, a.privilege_type, a.is_grantable)
              FROM pg_catalog.aclexplode(
                       COALESCE(_alias.proacl, pg_catalog.acldefault('f', _alias_owner))
                   ) AS a
             ORDER BY 1
        )
    ) THEN
        RETURN;
    END IF;

    -- Trusted extension scripts execute under SECURITY_LOCAL_USERID_CHANGE when
    -- invoked by a non-superuser owner, where SET LOCAL ROLE is disallowed.
    -- Replay grants from other grantors through a temporary SECURITY DEFINER
    -- helper owned by each grantor instead of modifying the role GUC.
    IF pg_catalog.to_regprocedure(
           pg_catalog.format('%I._fasttrun_2_0_acl_replay_helper(pg_catalog.text)', _schema)
       ) IS NOT NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = pg_catalog.format(
                'fasttrun upgrade helper name conflict in schema %I', _schema);
    END IF;

    FOR _grant IN
        SELECT DISTINCT grantor, grantee
          FROM pg_catalog.aclexplode(
              COALESCE(_canon.proacl, pg_catalog.acldefault('f', _canon.proowner)))
    LOOP
        _stmt := pg_catalog.format(
            'REVOKE ALL ON FUNCTION %I.fasttruncate(pg_catalog.text) FROM %s',
            _schema,
            CASE WHEN _grant.grantee = 0 THEN 'PUBLIC'
                 ELSE pg_catalog.quote_ident(pg_catalog.pg_get_userbyid(_grant.grantee))
            END);
        IF _grant.grantor = _exec_role THEN
            EXECUTE _stmt;
        ELSE
            IF NOT _helper_created THEN
                EXECUTE pg_catalog.format(
                    'CREATE FUNCTION %I._fasttrun_2_0_acl_replay_helper(_sql pg_catalog.text) '
                    'RETURNS void LANGUAGE plpgsql SECURITY DEFINER '
                    'SET search_path = pg_catalog, pg_temp '
                    'AS $h$ BEGIN EXECUTE _sql; END $h$',
                    _schema);
                EXECUTE pg_catalog.format(
                    'REVOKE ALL ON FUNCTION %I._fasttrun_2_0_acl_replay_helper(pg_catalog.text) FROM PUBLIC',
                    _schema);
                _helper_created := true;
            END IF;
            EXECUTE pg_catalog.format(
                'ALTER FUNCTION %I._fasttrun_2_0_acl_replay_helper(pg_catalog.text) OWNER TO %I',
                _schema, pg_catalog.pg_get_userbyid(_grant.grantor));
            BEGIN
                EXECUTE pg_catalog.format(
                    'SELECT %I._fasttrun_2_0_acl_replay_helper($1)', _schema)
                    USING _stmt;
            EXCEPTION WHEN insufficient_privilege THEN
                NULL;
            END;
        END IF;
    END LOOP;

    LOOP
        _progressed := false;
        FOR _grant IN
            SELECT g.rn, g.grantor, g.grantee, g.is_grantable
              FROM (
                  SELECT pg_catalog.row_number() OVER (
                             ORDER BY (a.grantor = _alias_owner) DESC,
                                      a.is_grantable DESC,
                                      a.grantor,
                                      a.grantee
                         ) AS rn,
                         a.grantor,
                         a.grantee,
                         a.is_grantable
                    FROM pg_catalog.aclexplode(
                             COALESCE(_alias.proacl, pg_catalog.acldefault('f', _alias_owner))
                         ) AS a
                   WHERE a.privilege_type = 'EXECUTE'
              ) AS g
             WHERE NOT (g.rn::integer = ANY(_applied))
             ORDER BY g.rn
        LOOP
            IF _grant.grantor <> _alias_owner
               AND NOT EXISTS (
                   SELECT 1
                     FROM pg_catalog.pg_proc p,
                          pg_catalog.aclexplode(
                              COALESCE(p.proacl, pg_catalog.acldefault('f', p.proowner))
                          ) AS cur
                    WHERE p.oid = _canon.oid
                      AND cur.privilege_type = 'EXECUTE'
                      AND cur.is_grantable
                      AND (cur.grantee = _grant.grantor
                           OR cur.grantee = 0
                           OR pg_catalog.pg_has_role(_grant.grantor, cur.grantee, 'USAGE'))
               ) THEN
                CONTINUE;
            END IF;
            _stmt := pg_catalog.format(
                'GRANT EXECUTE ON FUNCTION %I.fasttruncate(pg_catalog.text) TO %s%s',
                _schema,
                CASE WHEN _grant.grantee = 0 THEN 'PUBLIC'
                     ELSE pg_catalog.quote_ident(pg_catalog.pg_get_userbyid(_grant.grantee))
                END,
                CASE WHEN _grant.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END);
            IF _grant.grantor = _exec_role THEN
                EXECUTE _stmt;
                _applied := pg_catalog.array_append(_applied, _grant.rn::integer);
                _progressed := true;
            ELSE
                IF NOT _helper_created THEN
                    EXECUTE pg_catalog.format(
                        'CREATE FUNCTION %I._fasttrun_2_0_acl_replay_helper(_sql pg_catalog.text) '
                        'RETURNS void LANGUAGE plpgsql SECURITY DEFINER '
                        'SET search_path = pg_catalog, pg_temp '
                        'AS $h$ BEGIN EXECUTE _sql; END $h$',
                        _schema);
                    EXECUTE pg_catalog.format(
                        'REVOKE ALL ON FUNCTION %I._fasttrun_2_0_acl_replay_helper(pg_catalog.text) FROM PUBLIC',
                        _schema);
                    _helper_created := true;
                END IF;
                EXECUTE pg_catalog.format(
                    'ALTER FUNCTION %I._fasttrun_2_0_acl_replay_helper(pg_catalog.text) OWNER TO %I',
                    _schema, pg_catalog.pg_get_userbyid(_grant.grantor));
                BEGIN
                    EXECUTE pg_catalog.format(
                        'SELECT %I._fasttrun_2_0_acl_replay_helper($1)', _schema)
                        USING _stmt;
                    _applied := pg_catalog.array_append(_applied, _grant.rn::integer);
                    _progressed := true;
                EXCEPTION WHEN insufficient_privilege THEN
                    NULL;
                END;
            END IF;
        END LOOP;
        EXIT WHEN NOT _progressed;
    END LOOP;

    IF _helper_created THEN
        EXECUTE pg_catalog.format(
            'ALTER EXTENSION fasttrun DROP FUNCTION %I._fasttrun_2_0_acl_replay_helper(pg_catalog.text)',
            _schema);
        EXECUTE pg_catalog.format(
            'DROP FUNCTION %I._fasttrun_2_0_acl_replay_helper(pg_catalog.text)',
            _schema);
    END IF;

    IF (
        SELECT ARRAY(
            SELECT pg_catalog.format('%s/%s:%s:%s',
                       a.grantor, a.grantee, a.privilege_type, a.is_grantable)
              FROM pg_catalog.pg_proc p,
                   pg_catalog.aclexplode(
                       COALESCE(p.proacl, pg_catalog.acldefault('f', p.proowner))
                   ) AS a
             WHERE p.oid = _canon.oid
             ORDER BY 1
        )
    ) IS DISTINCT FROM (
        SELECT ARRAY(
            SELECT pg_catalog.format('%s/%s:%s:%s',
                       a.grantor, a.grantee, a.privilege_type, a.is_grantable)
              FROM pg_catalog.aclexplode(
                       COALESCE(_alias.proacl, pg_catalog.acldefault('f', _alias_owner))
                   ) AS a
             ORDER BY 1
        )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'fasttrun upgrade cannot preserve legacy alias ACL automatically',
            DETAIL = 'Replaying the C alias privilege graph onto canonical fasttruncate(text) did not reproduce the exact ACL.',
            HINT = 'Align privileges on fasttruncate_c(text) explicitly in a transaction before retrying ALTER EXTENSION fasttrun UPDATE.';
    END IF;
END
$sync_canonical_acl$;

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
