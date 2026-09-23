use anyhow::{Context, Result, bail};
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::{Builder as S3ConfigBuilder, Region};
use aws_smithy_types::byte_stream::ByteStream;
use base64::Engine;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::compute::ComputeNode;

#[derive(Clone)]
struct Config {
    database_name: String,
    tenant_id: String,
    namespace_id: String,
    source_generation: u64,
    endpoint: String,
    region: String,
    bucket: String,
}

#[derive(Debug)]
struct Intent {
    schema: String,
    entry_id: String,
    state_hash: String,
    submission_id: String,
    media_type: String,
    payload: Vec<u8>,
}

pub fn launch_if_configured(compute: Arc<ComputeNode>) {
    if std::env::var("LBFS_INDEX_EXPORT_ENABLED").as_deref() != Ok("true") {
        return;
    }
    if Config::from_compute(&compute).is_err() {
        return;
    }
    tokio::spawn(async move {
        loop {
            if let Err(error) = run(&compute).await {
                error!(%error, "LakebaseFS index exporter iteration failed");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}

async fn run(compute: &ComputeNode) -> Result<()> {
    let config = Config::from_compute(compute)?;
    let sdk = aws_config::defaults(BehaviorVersion::v2024_03_28())
        .region(Region::new(config.region.clone()))
        .load()
        .await;
    let s3_config = S3ConfigBuilder::from(&sdk)
        .endpoint_url(&config.endpoint)
        .force_path_style(false)
        .build();
    let s3 = aws_sdk_s3::Client::from_conf(s3_config);
    let mut connection_config = compute.tokio_conn_conf.clone();
    connection_config.dbname(&config.database_name);
    let (mut client, connection) = connection_config
        .connect(tokio_postgres::NoTls)
        .await
        .context("connect to local LakebaseFS database")?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            warn!(%error, "LakebaseFS exporter database connection ended");
        }
    });
    for schema in discover_schemas(&client).await? {
        let Some(intent) = claim(&mut client, &schema).await? else {
            continue;
        };
        match publish(&s3, &config, &intent).await {
            Ok((key, version, checksum)) => {
                acknowledge(&client, &intent, &key, &version, &checksum).await?;
                info!(schema=%intent.schema, entry=%intent.entry_id, %key, "exported LakebaseFS index input");
            }
            Err(error) => {
                retry(&client, &intent, &error.to_string()).await?;
                warn!(schema=%intent.schema, entry=%intent.entry_id, %error, "index input export will retry");
            }
        }
    }
    Ok(())
}

impl Config {
    fn from_compute(compute: &ComputeNode) -> Result<Self> {
        let state = compute
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("compute state lock poisoned"))?;
        let spec = state
            .pspec
            .as_ref()
            .context("compute spec is unavailable")?;
        let export = spec
            .spec
            .lakeon_index_export
            .as_ref()
            .context("index export is not configured for this compute assignment")?;
        let database_name = spec
            .spec
            .cluster
            .databases
            .first()
            .context("compute assignment has no database")?
            .name
            .clone();
        Ok(Self {
            database_name,
            tenant_id: export.tenant_id.clone(),
            namespace_id: export.namespace_id.clone(),
            source_generation: export.source_generation,
            endpoint: required("LBFS_INDEX_OBS_ENDPOINT")?,
            region: required("AWS_REGION")?,
            bucket: required("LBFS_INDEX_OBS_BUCKET")?,
        })
    }
}

async fn discover_schemas(client: &tokio_postgres::Client) -> Result<Vec<String>> {
    let rows = client.query("SELECT n.nspname FROM pg_namespace n JOIN pg_class c ON c.relnamespace=n.oid WHERE c.relname='index_export_outbox' AND c.relkind='r' AND n.nspname !~ '^pg_' ORDER BY n.nspname", &[]).await?;
    Ok(rows
        .into_iter()
        .map(|row| row.get(0))
        .filter(|name: &String| valid_ident(name))
        .collect())
}

async fn claim(client: &mut tokio_postgres::Client, schema: &str) -> Result<Option<Intent>> {
    let transaction = client.transaction().await?;
    let supersede_sql = format!(
        "UPDATE {s}.index_export_outbox o SET status='dead',lease_owner=NULL,lease_expires_at=NULL,last_error='superseded by a newer file revision',updated_at=now() WHERE o.status IN ('pending','uploading') AND NOT EXISTS (SELECT 1 FROM {s}.entries e JOIN {s}.file_contents f ON f.entry_id=e.entry_id WHERE e.entry_id=o.entry_id AND e.state_hash=o.source_state_hash)",
        s = quote(schema)
    );
    transaction.execute(&supersede_sql, &[]).await?;
    let sql = format!(
        "WITH candidate AS (SELECT entry_id,source_state_hash,submission_id,media_type FROM {s}.index_export_outbox WHERE status IN ('pending','uploading') AND available_at<=now() AND (lease_expires_at IS NULL OR lease_expires_at<=now()) ORDER BY available_at,entry_id FOR UPDATE SKIP LOCKED LIMIT 1), claimed AS (UPDATE {s}.index_export_outbox o SET status='uploading',attempts=attempts+1,lease_owner=$1,lease_expires_at=now()+interval '2 minutes',updated_at=now() FROM candidate c WHERE o.entry_id=c.entry_id AND o.source_state_hash=c.source_state_hash RETURNING o.entry_id,encode(o.source_state_hash,'hex'),o.submission_id,o.media_type) SELECT c.entry_id::text,c.encode,c.submission_id,c.media_type,f.data FROM claimed c JOIN {s}.file_contents f ON f.entry_id=c.entry_id JOIN {s}.entries e ON e.entry_id=c.entry_id AND encode(e.state_hash,'hex')=c.encode",
        s = quote(schema)
    );
    let owner = std::env::var("HOSTNAME").unwrap_or_else(|_| "compute_ctl".into());
    let row = transaction.query_opt(&sql, &[&owner]).await?;
    transaction.commit().await?;
    Ok(row.map(|row| Intent {
        schema: schema.into(),
        entry_id: row.get(0),
        state_hash: row.get(1),
        submission_id: row.get(2),
        media_type: row.get(3),
        payload: row.get(4),
    }))
}

async fn publish(
    s3: &aws_sdk_s3::Client,
    config: &Config,
    intent: &Intent,
) -> Result<(String, String, String)> {
    let checksum = hex::encode(Sha256::digest(&intent.payload));
    let source_id = deterministic_source_id(&config.tenant_id, &intent.schema);
    let key = object_key(config, intent, source_id);
    let mut metadata = HashMap::new();
    metadata.insert("x-index-contract".into(), "document-object/v1".into());
    metadata.insert("x-index-input-kind".into(), "raw_file".into());
    metadata.insert("x-index-media-type".into(), intent.media_type.clone());
    metadata.insert("x-index-payload-sha256".into(), checksum.clone());
    let encoded_revision = segment(intent.state_hash.clone());
    metadata.insert("x-index-source-revision".into(), encoded_revision.clone());
    metadata.insert("x-index-created-at".into(), chrono::Utc::now().to_rfc3339());
    let response = s3
        .put_object()
        .bucket(&config.bucket)
        .key(&key)
        .set_metadata(Some(metadata))
        .content_type(&intent.media_type)
        .body(ByteStream::from(intent.payload.clone()))
        .send()
        .await?;
    let version = response.version_id().unwrap_or_default().to_string();
    if version.is_empty() {
        bail!("OBS bucket did not return an immutable version ID");
    }
    let head = s3
        .head_object()
        .bucket(&config.bucket)
        .key(&key)
        .version_id(&version)
        .send()
        .await?;
    if head.content_length() != Some(intent.payload.len() as i64)
        || head
            .metadata()
            .and_then(|value| value.get("x-index-payload-sha256"))
            != Some(&checksum)
        || head
            .metadata()
            .and_then(|value| value.get("x-index-source-revision"))
            != Some(&encoded_revision)
    {
        bail!("OBS immutable object verification failed");
    }
    Ok((key, version, checksum))
}

fn object_key(config: &Config, intent: &Intent, source_id: Uuid) -> String {
    let values = [
        config.tenant_id.clone(),
        config.namespace_id.clone(),
        source_id.to_string(),
        "1".into(),
        config.source_generation.to_string(),
        intent.schema.clone(),
        intent.entry_id.clone(),
        intent.state_hash.clone(),
        intent.submission_id.clone(),
    ];
    format!(
        "index/v1/inputs/{}/payload",
        values
            .into_iter()
            .map(segment)
            .collect::<Vec<_>>()
            .join("/")
    )
}

fn segment(value: String) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value.as_bytes())
}

fn deterministic_source_id(tenant: &str, collection: &str) -> Uuid {
    let digest =
        Sha256::digest(format!("lakebasefs-index-source-v1\0{tenant}\0{collection}").as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

async fn acknowledge(
    client: &tokio_postgres::Client,
    intent: &Intent,
    key: &str,
    version: &str,
    checksum: &str,
) -> Result<()> {
    let sql = format!(
        "UPDATE {s}.index_export_outbox SET status='exported',object_key=$1,object_version_id=$2,object_checksum=$3,lease_owner=NULL,lease_expires_at=NULL,last_error=NULL,updated_at=now() WHERE entry_id=$4::uuid AND source_state_hash=decode($5,'hex') AND status='uploading'",
        s = quote(&intent.schema)
    );
    if client
        .execute(
            &sql,
            &[
                &key,
                &version,
                &checksum,
                &intent.entry_id,
                &intent.state_hash,
            ],
        )
        .await?
        != 1
    {
        bail!("export acknowledgement lost its claim");
    }
    Ok(())
}

async fn retry(client: &tokio_postgres::Client, intent: &Intent, error: &str) -> Result<()> {
    let sql = format!(
        "UPDATE {s}.index_export_outbox SET status='pending',available_at=now()+interval '10 seconds',lease_owner=NULL,lease_expires_at=NULL,last_error=$1,updated_at=now() WHERE entry_id=$2::uuid AND source_state_hash=decode($3,'hex')",
        s = quote(&intent.schema)
    );
    client
        .execute(
            &sql,
            &[
                &error.chars().take(1000).collect::<String>(),
                &intent.entry_id,
                &intent.state_hash,
            ],
        )
        .await?;
    Ok(())
}

fn quote(value: &str) -> String {
    assert!(valid_ident(value));
    format!("\"{value}\"")
}
fn valid_ident(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .enumerate()
            .all(|(i, b)| b == b'_' || b.is_ascii_lowercase() || (i > 0 && b.is_ascii_digit()))
}
fn required(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("{name} is required"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_identity_and_object_key_are_stable() {
        let source = deterministic_source_id("tenant-a", "folder_rf_a");
        assert_eq!(source.to_string(), "c3e9b970-29df-506e-ab86-6f7007629879");
        assert_eq!(source, deterministic_source_id("tenant-a", "folder_rf_a"));
        assert_ne!(source, deterministic_source_id("tenant-a", "folder_rf_b"));
        let config = Config {
            database_name: "x".into(),
            tenant_id: "tenant-a".into(),
            namespace_id: "lakebasefs".into(),
            source_generation: 7,
            endpoint: "x".into(),
            region: "x".into(),
            bucket: "x".into(),
        };
        let intent = Intent {
            schema: "folder_rf_a".into(),
            entry_id: Uuid::nil().to_string(),
            state_hash: "11".repeat(32),
            submission_id: "event:1".into(),
            media_type: "text/markdown".into(),
            payload: vec![],
        };
        assert!(object_key(&config, &intent, source).starts_with("index/v1/inputs/"));
        assert_eq!(object_key(&config, &intent, source).split('/').count(), 13);
        let encoded_revision = segment(intent.state_hash.clone());
        assert_ne!(encoded_revision, intent.state_hash);
        assert_eq!(
            String::from_utf8(
                base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(encoded_revision)
                    .unwrap()
            )
            .unwrap(),
            intent.state_hash
        );
    }
}
