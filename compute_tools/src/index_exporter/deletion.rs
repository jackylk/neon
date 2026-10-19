//! Immutable deletion proof outlives its source entry and all content relations.
use super::*;
pub(super) const MEDIA: &str = "application/vnd.lakeon.canonical-delete+json;version=1";

pub(super) async fn claim(
    tx: &tokio_postgres::Transaction<'_>,
    schema: &str,
    inc: i64,
    version: i64,
) -> Result<Option<Intent>> {
    let s = quote(schema);
    let token = Uuid::new_v4().to_string();
    let sql = format!(
        r#"
        WITH candidate AS (
            SELECT i.event_id FROM {s}.index_export_intents i
            WHERE i.incarnation=$1 AND i.source_config_version=$2 AND i.job_kind='tombstone'
              AND i.status IN ('pending','uploading') AND i.available_at<=clock_timestamp()
              AND (i.lease_expires_at IS NULL OR i.lease_expires_at<=clock_timestamp())
              AND NOT EXISTS(SELECT 1 FROM {s}.entries e WHERE e.entry_id=i.entry_id)
            ORDER BY i.available_at,i.event_id FOR UPDATE OF i SKIP LOCKED LIMIT 1
        ), claimed AS (
            UPDATE {s}.index_export_intents i
            SET status='uploading',attempts=attempts+1,lease_token=$3::text::uuid,
                lease_expires_at=clock_timestamp()+interval '2 minutes',updated_at=clock_timestamp()
            FROM candidate c WHERE i.event_id=c.event_id RETURNING i.*
        ) SELECT event_id::text,entry_id::text,encode(source_state_hash,'hex'),encode(fingerprint,'hex')
          FROM claimed"#
    );
    let row = tx.query_opt(&sql, &[&inc, &version, &token]).await?;
    let Some(row) = row else { return Ok(None) };
    let event: String = row.get(0);
    let entry: String = row.get(1);
    let state: String = row.get(2);
    let fingerprint: String = row.get(3);
    let bytes = serde_json::to_vec(
        &serde_json::json!({"schema":"canonical-delete/v1","event_id":event,"source_state_hash":state,"fingerprint":fingerprint}),
    )?;
    let mut file = tokio::fs::File::from_std(tempfile::tempfile()?);
    file.write_all(&bytes).await?;
    file.flush().await?;
    Ok(Some(Intent {
        schema: schema.into(),
        entry_id: entry,
        state_hash: state,
        lexical_profile_hash: String::new(),
        submission_id: event.clone(),
        media_type: MEDIA.into(),
        payload: Spool {
            file,
            len: bytes.len(),
            checksum: hex::encode(Sha256::digest(&bytes)),
        },
        lease_owner: token,
        event: EventIdentity {
            event_id: event,
            fingerprint,
            incarnation: inc,
            source_config_version: version,
            job_kind: "tombstone".into(),
            content_hash: String::new(),
        },
    }))
}
