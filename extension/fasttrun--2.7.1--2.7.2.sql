\echo Use "ALTER EXTENSION fasttrun UPDATE TO '2.7.2'" to load this file. \quit

-- Close PUBLIC EXECUTE on a leftover extension-owned fasttruncate_c(text) alias
-- when an earlier upgrade from 2.0 created it with default privileges after
-- PUBLIC EXECUTE had already been revoked from fasttruncate(text).
DO $sync_legacy_alias_public_acl$
DECLARE
    _extension oid;
    _namespace oid;
    _schema text;
    _canon pg_catalog.pg_proc;
    _alias pg_catalog.pg_proc;
    _canon_public boolean;
    _alias_public boolean;
    _remaining_grantors text;
BEGIN
    SELECT e.oid, e.extnamespace, n.nspname
      INTO STRICT _extension, _namespace, _schema
      FROM pg_catalog.pg_extension e
      JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
     WHERE e.extname = 'fasttrun';

    SELECT p.* INTO _canon
      FROM pg_catalog.pg_proc p
      JOIN pg_catalog.pg_depend d
        ON d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
       AND d.objid = p.oid AND d.objsubid = 0
       AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
       AND d.refobjid = _extension AND d.deptype = 'e'
     WHERE p.pronamespace = _namespace
       AND p.proname = 'fasttruncate'
       AND p.pronargs = 1
       AND p.proargtypes[0] = 'pg_catalog.text'::pg_catalog.regtype;
    IF NOT FOUND THEN
        RETURN;
    END IF;

    SELECT p.* INTO _alias
      FROM pg_catalog.pg_proc p
      JOIN pg_catalog.pg_depend d
        ON d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
       AND d.objid = p.oid AND d.objsubid = 0
       AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
       AND d.refobjid = _extension AND d.deptype = 'e'
     WHERE p.pronamespace = _namespace
       AND p.proname = 'fasttruncate_c'
       AND p.pronargs = 1
       AND p.proargtypes[0] = 'pg_catalog.text'::pg_catalog.regtype;
    IF NOT FOUND THEN
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT 1
          FROM pg_catalog.aclexplode(
              COALESCE(_canon.proacl, pg_catalog.acldefault('f', _canon.proowner))
          ) a
         WHERE a.grantee = 0 AND a.privilege_type = 'EXECUTE'
    ) INTO _canon_public;

    SELECT EXISTS (
        SELECT 1
          FROM pg_catalog.aclexplode(
              COALESCE(_alias.proacl, pg_catalog.acldefault('f', _alias.proowner))
          ) a
         WHERE a.grantee = 0 AND a.privilege_type = 'EXECUTE'
    ) INTO _alias_public;

    IF NOT _canon_public AND _alias_public THEN
        EXECUTE pg_catalog.format(
            'REVOKE EXECUTE ON FUNCTION %I.fasttruncate_c(pg_catalog.text) FROM PUBLIC',
            _schema
        );
        SELECT pg_catalog.string_agg(
                   DISTINCT pg_catalog.quote_ident(pg_catalog.pg_get_userbyid(a.grantor)),
                   ', '
               )
          INTO _remaining_grantors
          FROM pg_catalog.pg_proc p,
               pg_catalog.aclexplode(
                   COALESCE(p.proacl, pg_catalog.acldefault('f', p.proowner))
               ) a
         WHERE p.oid = _alias.oid
           AND a.grantee = 0
           AND a.privilege_type = 'EXECUTE';
        IF _remaining_grantors IS NOT NULL THEN
            RAISE WARNING
                'fasttrun: %.fasttruncate_c(text) still grants EXECUTE to PUBLIC via grantor(s) % while %.fasttruncate(text) does not; revoke PUBLIC EXECUTE as those grantor role(s)',
                pg_catalog.quote_ident(_schema),
                _remaining_grantors,
                pg_catalog.quote_ident(_schema);
        END IF;
    END IF;
END
$sync_legacy_alias_public_acl$;
