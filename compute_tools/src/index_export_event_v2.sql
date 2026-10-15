CREATE TABLE IF NOT EXISTS lbfs_export_config (
    singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
    source_config_version bigint NOT NULL CHECK(source_config_version>0));
INSERT INTO lbfs_export_config VALUES(true,1) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS index_export_intents (
    event_id uuid PRIMARY KEY,
    entry_id uuid NOT NULL,
    source_state_hash bytea NOT NULL CHECK(octet_length(source_state_hash)=32),
    fingerprint bytea NOT NULL CHECK(octet_length(fingerprint)=32),
    incarnation bigint NOT NULL CHECK(incarnation>0),
    source_config_version bigint NOT NULL CHECK(source_config_version>0),
    job_kind text NOT NULL CHECK(job_kind IN ('file_content','record_content','semantic_content','tombstone','repair')),
    content_hash bytea CHECK(content_hash IS NULL OR octet_length(content_hash)=32),
    lexical_profile_hash bytea CHECK(lexical_profile_hash IS NULL OR octet_length(lexical_profile_hash)=32),
    embedding_profile_id text,
    media_type text NOT NULL DEFAULT 'application/octet-stream',
    status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','uploading','exported','dead')),
    attempts integer NOT NULL DEFAULT 0 CHECK(attempts>=0),
    available_at timestamptz NOT NULL DEFAULT now(),
    lease_token uuid,
    lease_expires_at timestamptz,
    object_key text,
    object_version_id text,
    object_checksum text CHECK(object_checksum IS NULL OR object_checksum ~ '^[0-9a-f]{64}$'),
    object_size bigint CHECK(object_size>=0),
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((status='exported')=(object_key IS NOT NULL AND object_version_id IS NOT NULL
          AND object_checksum IS NOT NULL AND object_size IS NOT NULL)),
    CHECK (status<>'uploading' OR (lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)));
CREATE INDEX IF NOT EXISTS index_export_intents_ready
    ON index_export_intents(available_at,event_id) WHERE status IN ('pending','uploading');
REVOKE ALL ON index_export_intents,lbfs_export_config FROM PUBLIC;

CREATE OR REPLACE FUNCTION lbfs_export_immutable()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $immutable$
BEGIN
    IF (NEW.event_id,NEW.entry_id,NEW.source_state_hash,NEW.fingerprint,NEW.incarnation,
        NEW.source_config_version,NEW.job_kind,NEW.content_hash,NEW.lexical_profile_hash,
        NEW.media_type,NEW.created_at) IS DISTINCT FROM
       (OLD.event_id,OLD.entry_id,OLD.source_state_hash,OLD.fingerprint,OLD.incarnation,
        OLD.source_config_version,OLD.job_kind,OLD.content_hash,OLD.lexical_profile_hash,
        OLD.media_type,OLD.created_at) OR
       (OLD.embedding_profile_id IS NOT NULL AND NEW.embedding_profile_id IS DISTINCT FROM OLD.embedding_profile_id) THEN
        RAISE EXCEPTION 'EXPORT_EVENT_IMMUTABLE' USING ERRCODE='23514';
    END IF;
    IF OLD.status='exported' AND
       (NEW.status,NEW.object_key,NEW.object_version_id,NEW.object_checksum,NEW.object_size,NEW.lease_token)
       IS DISTINCT FROM
       (OLD.status,OLD.object_key,OLD.object_version_id,OLD.object_checksum,OLD.object_size,OLD.lease_token) THEN
        RAISE EXCEPTION 'EXPORT_RECEIPT_IMMUTABLE' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $immutable$;
REVOKE ALL ON FUNCTION lbfs_export_immutable() FROM PUBLIC;
DROP TRIGGER IF EXISTS lbfs_export_immutable ON index_export_intents;
CREATE TRIGGER lbfs_export_immutable BEFORE UPDATE ON index_export_intents
    FOR EACH ROW EXECUTE FUNCTION lbfs_export_immutable();
