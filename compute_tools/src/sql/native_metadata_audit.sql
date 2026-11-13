SET search_path=pg_catalog;
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_settings
        WHERE name='pgaudit.lakeon_metadata_version' AND setting='1'
          AND context='internal' AND NOT pending_restart
    ) THEN
        RAISE EXCEPTION 'Verified metadata audit producer is not loaded';
    END IF;
END $$;
CREATE SCHEMA IF NOT EXISTS lakeon_audit AUTHORIZATION cloud_admin;
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner
        WHERE n.nspname='lakeon_audit' AND r.rolname='cloud_admin'
    ) THEN
        RAISE EXCEPTION 'Managed audit namespace ownership mismatch';
    END IF;
END $$;
REVOKE CREATE ON SCHEMA lakeon_audit FROM PUBLIC;
GRANT USAGE ON SCHEMA lakeon_audit TO PUBLIC;
CREATE EXTENSION IF NOT EXISTS pgaudit WITH SCHEMA lakeon_audit;
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_extension e JOIN pg_namespace n ON n.oid=e.extnamespace
        WHERE e.extname='pgaudit' AND n.nspname='lakeon_audit'
    ) THEN
        RAISE EXCEPTION 'Audit extension is outside the managed namespace';
    END IF;
END $$;

RESET search_path;
