//! One bounded immutable append-log record, independent of the mutable file head.
use super::*;
const MAX_RECORD_BYTES: usize = 1_048_576;
const MAX_JSON_BYTES: usize = 8 * MAX_RECORD_BYTES;
pub(super) const MEDIA: &str = "application/vnd.lakeon.canonical-record+json;version=1";

pub(super) async fn spool(
    tx: &tokio_postgres::Transaction<'_>,
    schema: &str,
    entry: &str,
    state: &str,
    content: &str,
) -> Result<Spool> {
    let row=tx.query_one(&format!("SELECT seq,record_id::text,line_no,byte_start,byte_end,CASE WHEN octet_length(raw_line)<=1048576 THEN raw_line END,CASE WHEN octet_length(text)<=1048576 THEN text END FROM {}.session_records WHERE entry_id=$1::text::uuid AND prefix_state_hash=decode($2,'hex') AND record_hash=decode($3,'hex')",quote(schema)),&[&entry,&state,&content]).await?;
    let seq: i64 = row.try_get(0)?;
    let id: String = row.try_get(1)?;
    let line: i64 = row.try_get(2)?;
    let start: i64 = row.try_get(3)?;
    let end: i64 = row.try_get(4)?;
    let raw: Vec<u8> = row.try_get(5)?;
    let text: String = row.try_get(6)?;
    if raw.is_empty()
        || raw.len() > MAX_RECORD_BYTES
        || text.len() > MAX_RECORD_BYTES
        || seq < 1
        || line < 1
        || start < 0
        || end.checked_sub(start) != Some(raw.len() as i64)
    {
        bail!("invalid record size or coordinates");
    }
    let domain = b"lbfs-append-record-v1";
    let mut hash = Sha256::new();
    hash.update((domain.len() as i32).to_be_bytes());
    hash.update(domain);
    hash.update((raw.len() as u64).to_be_bytes());
    hash.update(&raw);
    if hex::encode(hash.finalize()) != content {
        bail!("record raw-line checksum mismatch");
    }
    let bytes = serde_json::to_vec(
        &serde_json::json!({"schema":"canonical-record/v1","source_state_hash":state,"record_id":id,"seq":seq,"line_no":line,"byte_start":start,"byte_end":end,"record_hash":content,"text":text}),
    )?;
    if bytes.len() > MAX_JSON_BYTES {
        bail!("record JSON exceeds 8MiB");
    }
    let mut file = tokio::fs::File::from_std(tempfile::tempfile()?);
    file.write_all(&bytes).await?;
    file.flush().await?;
    Ok(Spool {
        file,
        len: bytes.len(),
        checksum: hex::encode(Sha256::digest(&bytes)),
    })
}

pub(super) async fn claim(
    tx: tokio_postgres::Transaction<'_>,
    schema: &str,
    inc: i64,
    version: i64,
) -> Result<Option<Intent>> {
    let s = quote(schema);
    tx.execute(&format!("UPDATE {s}.index_export_intents i SET status='dead',lease_token=NULL,lease_expires_at=NULL,last_error='record no longer retained',updated_at=clock_timestamp() WHERE incarnation=$1 AND source_config_version=$2 AND job_kind='record_content' AND status IN ('pending','uploading') AND NOT EXISTS(SELECT 1 FROM {s}.session_records r JOIN {s}.entries e USING(entry_id) WHERE r.entry_id=i.entry_id AND r.prefix_state_hash=i.source_state_hash AND r.record_hash=i.content_hash AND i.fingerprint=r.record_hash)"),&[&inc,&version]).await?;
    let token = Uuid::new_v4().to_string();
    let row=tx.query_opt(&format!("WITH candidate AS (SELECT i.event_id FROM {s}.index_export_intents i JOIN {s}.session_records r ON r.entry_id=i.entry_id AND r.prefix_state_hash=i.source_state_hash AND r.record_hash=i.content_hash AND i.fingerprint=r.record_hash JOIN {s}.entries e ON e.entry_id=r.entry_id WHERE i.incarnation=$1 AND i.source_config_version=$2 AND i.job_kind='record_content' AND i.status IN ('pending','uploading') AND i.available_at<=clock_timestamp() AND (i.lease_expires_at IS NULL OR i.lease_expires_at<=clock_timestamp()) ORDER BY i.available_at,i.event_id FOR UPDATE OF i SKIP LOCKED LIMIT 1), c AS (UPDATE {s}.index_export_intents i SET status='uploading',attempts=attempts+1,lease_token=$3::text::uuid,lease_expires_at=clock_timestamp()+interval '2 minutes',updated_at=clock_timestamp() FROM candidate WHERE i.event_id=candidate.event_id RETURNING i.*) SELECT c.event_id::text,c.entry_id::text,encode(c.source_state_hash,'hex'),encode(c.fingerprint,'hex'),c.incarnation,c.source_config_version,c.job_kind,''::text,'{MEDIA}'::text,0::int,encode(c.content_hash,'hex') FROM c"),&[&inc,&version,&token]).await?;
    let payload = if let Some(row) = &row {
        match spool_claim(&tx, schema, row, &token, true).await {
            Ok(p) => Some(p),
            Err(e) => {
                tx.commit().await?;
                return Err(e);
            }
        }
    } else {
        None
    };
    tx.commit().await?;
    Ok(row.map(|r| Intent {
        schema: schema.into(),
        entry_id: r.get(1),
        state_hash: r.get(2),
        lexical_profile_hash: String::new(),
        submission_id: r.get(0),
        media_type: MEDIA.into(),
        payload: payload.unwrap(),
        lease_owner: token,
        event: Some(EventIdentity {
            event_id: r.get(0),
            fingerprint: r.get(3),
            incarnation: r.get(4),
            source_config_version: r.get(5),
            job_kind: r.get(6),
            content_hash: r.get(10),
        }),
    }))
}
