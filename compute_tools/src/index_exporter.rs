use anyhow::{Context, Result, bail};
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::{Builder as S3ConfigBuilder, Region};
use aws_smithy_types::byte_stream::ByteStream;
use base64::Engine;
use chrono::SecondsFormat;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::compute::ComputeNode;

mod canonical;

#[derive(Clone)]
struct Config {
    database_name: String,
    tenant_id: String,
    namespace_id: String,
    source_generation: u64,
    source_config_version: u64,
    journal: bool,
    endpoint: String,
    region: String,
    bucket: String,
}

#[derive(Debug)]
struct Intent {
    schema: String,
    entry_id: String,
    state_hash: String,
    lexical_profile_hash: String,
    submission_id: String,
    media_type: String,
    payload: Spool,
    lease_owner: String,
    event: Option<EventIdentity>,
}

// Match LakebaseFS file_contents_max_file_bytes; do not let a corrupt or old
// schema force unbounded disk/memory use in the exporter.
const MAX_FILE_BYTES: usize = 67_108_864;
const SOURCE_CHUNK_BYTES: usize = 1_048_576;

#[derive(Debug)]
struct Spool {
    // Anonymous file: no path survives cancellation, error, or process death.
    file: tokio::fs::File,
    len: usize,
    checksum: String,
}

impl Spool {
    async fn body(&self) -> Result<ByteStream> {
        let mut file = self.file.try_clone().await?;
        file.seek(std::io::SeekFrom::Start(0)).await?;
        // A file body is not SDK-replayable. publish reconciles ambiguous PUTs
        // by exact version; the durable leased intent owns subsequent retries.
        Ok(ByteStream::read_from()
            .file(file)
            .length(aws_smithy_types::byte_stream::Length::Exact(
                self.len as u64,
            ))
            .buffer_size(SOURCE_CHUNK_BYTES)
            .build()
            .await?)
    }
}

async fn spool_source(
    transaction: &tokio_postgres::Transaction<'_>,
    schema: &str,
    entry_id: &str,
    len: i32,
) -> Result<Spool> {
    let len = usize::try_from(len).context("negative source length")?;
    if len > MAX_FILE_BYTES {
        bail!("source exceeds LakebaseFS 64MiB file limit");
    }
    let mut file = tokio::fs::File::from_std(tempfile::tempfile()?);
    let mut hash = Sha256::new();
    let sql = format!(
        "SELECT substring(data from $2 for $3) FROM {}.file_contents WHERE entry_id=$1::text::uuid",
        quote(schema)
    );
    let mut offset = 0;
    while offset < len {
        let count = (len - offset).min(SOURCE_CHUNK_BYTES);
        let row = transaction
            .query_one(&sql, &[&entry_id, &((offset + 1) as i32), &(count as i32)])
            .await?;
        let chunk: Vec<u8> = row.get(0);
        if chunk.len() != count {
            bail!("source changed size while materializing export input");
        }
        hash.update(&chunk);
        file.write_all(&chunk).await?;
        offset += count;
    }
    file.flush().await?;
    Ok(Spool {
        file,
        len,
        checksum: hex::encode(hash.finalize()),
    })
}

#[derive(Debug)]
struct EventIdentity {
    event_id: String,
    fingerprint: String,
    incarnation: i64,
    source_config_version: i64,
    job_kind: String,
    content_hash: String,
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
    let mut schemas = discover_schemas(&client, config.journal).await?.into_iter();
    while let Some(intent) = next_claim(&mut client, &mut schemas, &config).await {
        let published =
            tokio::time::timeout(Duration::from_secs(60), publish(&s3, &config, &intent))
                .await
                .unwrap_or_else(|_| Err(anyhow::anyhow!("input publication timed out")));
        match published {
            Ok((key, version, checksum)) => {
                acknowledge(&mut client, &intent, &key, &version, &checksum).await?;
                info!(schema=%intent.schema, entry=%intent.entry_id, %key, "exported LakebaseFS index input");
            }
            Err(error) => {
                retry(&mut client, &intent, &error.to_string()).await?;
                warn!(schema=%intent.schema, entry=%intent.entry_id, %error, "index input export will retry");
            }
        }
    }
    Ok(())
}

async fn next_claim(
    client: &mut tokio_postgres::Client,
    schemas: &mut std::vec::IntoIter<String>,
    config: &Config,
) -> Option<Intent> {
    for schema in schemas {
        let result = if config.journal {
            claim_journal(client, &schema, config).await
        } else {
            claim(client, &schema).await
        };
        match result {
            Ok(Some(intent)) => return Some(intent),
            Ok(None) => {}
            Err(error) => warn!(%schema, %error, "source claim deferred; continuing other folders"),
        }
    }
    None
}

// Roll back only materialization work, keeping the candidate lock and attempt
// until a full-identity/lease guarded backoff is committed. A bad input must not
// occupy the head on every iteration. The outer transaction commits this backoff
// even though no object may be published or ACKed for this failed materialization.
async fn spool_claim(
    transaction: &tokio_postgres::Transaction<'_>,
    schema: &str,
    row: &tokio_postgres::Row,
    token: &str,
    journal: bool,
) -> Result<Spool> {
    transaction
        .batch_execute("SAVEPOINT export_payload")
        .await?;
    let (entry, len) = if journal { (1, 9) } else { (0, 5) };
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        if journal && row.get::<_, String>(6) == "semantic_content" {
            canonical::spool(
                transaction,
                schema,
                &row.get::<_, String>(1),
                &row.get::<_, String>(2),
                &row.get::<_, String>(11),
                &row.get::<_, String>(7),
            )
            .await
        } else {
            spool_source(
                transaction,
                schema,
                row.get::<_, String>(entry).as_str(),
                row.get(len),
            )
            .await
        }
    })
    .await
    .unwrap_or_else(|_| Err(anyhow::anyhow!("source materialization timed out")));
    if let Err(error) = &result {
        transaction
            .batch_execute("ROLLBACK TO SAVEPOINT export_payload")
            .await?;
        defer_materialization(transaction, schema, row, token, journal, &error.to_string()).await?;
    }
    result
}

async fn defer_materialization(
    transaction: &tokio_postgres::Transaction<'_>,
    schema: &str,
    row: &tokio_postgres::Row,
    token: &str,
    journal: bool,
    error: &str,
) -> Result<()> {
    let error: String = error.chars().take(1000).collect();
    let s = quote(schema);
    let changed = if journal {
        transaction.execute(&format!("UPDATE {s}.index_export_intents SET status='pending',available_at=clock_timestamp()+interval '10 seconds',lease_token=NULL,lease_expires_at=NULL,last_error=$1,updated_at=clock_timestamp() WHERE event_id=$2::text::uuid AND entry_id=$3::text::uuid AND source_state_hash=decode($4,'hex') AND fingerprint=decode($5,'hex') AND incarnation=$6 AND source_config_version=$7 AND job_kind=$8 AND status='uploading' AND lease_token=$9::text::uuid AND lease_expires_at>clock_timestamp()"),
            &[&error,&row.get::<_,String>(0),&row.get::<_,String>(1),&row.get::<_,String>(2),&row.get::<_,String>(3),&row.get::<_,i64>(4),&row.get::<_,i64>(5),&row.get::<_,String>(6),&token]).await?
    } else {
        transaction.execute(&format!("UPDATE {s}.index_export_outbox SET status='pending',available_at=clock_timestamp()+interval '10 seconds',lease_owner=NULL,lease_expires_at=NULL,last_error=$1,updated_at=clock_timestamp() WHERE entry_id=$2::text::uuid AND source_state_hash=decode($3,'hex') AND lexical_profile_hash=decode($4,'hex') AND submission_id=$5 AND lease_owner=$6 AND lease_expires_at>clock_timestamp() AND status='uploading'"),
            &[&error,&row.get::<_,String>(0),&row.get::<_,String>(1),&row.get::<_,String>(2),&row.get::<_,String>(3),&token]).await?
    };
    if changed != 1 {
        bail!("materialization backoff lost its claim");
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
        let journal = match export.protocol.as_deref() {
            None | Some("legacy-v1") => false,
            Some("event-v2") => true,
            Some(_) => bail!("unknown LakebaseFS export protocol"),
        };
        if journal
            && (export.workspace_incarnation != Some(export.source_generation)
                || export.source_generation == 0
                || export.source_generation > i64::MAX as u64
                || export
                    .source_config_version
                    .is_none_or(|version| version == 0 || version > i64::MAX as u64))
        {
            bail!("event-v2 requires explicit matching incarnation and source configuration");
        }
        Ok(Self {
            database_name,
            tenant_id: export.tenant_id.clone(),
            namespace_id: export.namespace_id.clone(),
            source_generation: export.source_generation,
            source_config_version: export.source_config_version.unwrap_or(1),
            journal,
            endpoint: required("LBFS_INDEX_OBS_ENDPOINT")?,
            region: required("AWS_REGION")?,
            bucket: required("LBFS_INDEX_OBS_BUCKET")?,
        })
    }
}

async fn discover_schemas(client: &tokio_postgres::Client, journal: bool) -> Result<Vec<String>> {
    let table = if journal {
        "index_export_intents"
    } else {
        "index_export_outbox"
    };
    let rows = client.query("SELECT n.nspname FROM pg_namespace n JOIN pg_class c ON c.relnamespace=n.oid WHERE c.relname=$1 AND c.relkind='r' AND n.nspname !~ '^pg_' ORDER BY n.nspname", &[&table]).await?;
    Ok(rows
        .into_iter()
        .map(|row| row.get(0))
        .filter(|name: &String| valid_ident(name))
        .collect())
}

async fn materialization_transaction(
    client: &mut tokio_postgres::Client,
) -> Result<tokio_postgres::Transaction<'_>> {
    let transaction = client
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .start()
        .await?;
    transaction.batch_execute("SET LOCAL statement_timeout='15s'; SET LOCAL idle_in_transaction_session_timeout='30s'").await?;
    Ok(transaction)
}

async fn claim(client: &mut tokio_postgres::Client, schema: &str) -> Result<Option<Intent>> {
    let transaction = materialization_transaction(client).await?;
    let supersede_sql = format!(
        "UPDATE {s}.index_export_outbox o SET status='dead',lease_owner=NULL,lease_expires_at=NULL,last_error='superseded by a newer file revision',updated_at=now() WHERE o.status IN ('pending','uploading') AND NOT EXISTS (SELECT 1 FROM {s}.entries e JOIN {s}.file_contents f ON f.entry_id=e.entry_id WHERE e.entry_id=o.entry_id AND e.state_hash=o.source_state_hash)",
        s = quote(schema)
    );
    transaction.execute(&supersede_sql, &[]).await?;
    let sql = format!(
        "WITH candidate AS (SELECT entry_id,source_state_hash,submission_id,media_type FROM {s}.index_export_outbox WHERE lexical_profile_hash IS NOT NULL AND status IN ('pending','uploading') AND available_at<=now() AND (lease_expires_at IS NULL OR lease_expires_at<=now()) ORDER BY available_at,entry_id FOR UPDATE SKIP LOCKED LIMIT 1), claimed AS (UPDATE {s}.index_export_outbox o SET status='uploading',attempts=attempts+1,lease_owner=$1,lease_expires_at=now()+interval '2 minutes',updated_at=now() FROM candidate c WHERE o.entry_id=c.entry_id AND o.source_state_hash=c.source_state_hash RETURNING o.entry_id,encode(o.source_state_hash,'hex') AS state_hash,encode(o.lexical_profile_hash,'hex') AS lexical_profile_hash,o.submission_id,o.media_type) SELECT c.entry_id::text,c.state_hash,c.lexical_profile_hash,c.submission_id,c.media_type,octet_length(f.data) FROM claimed c JOIN {s}.file_contents f ON f.entry_id=c.entry_id JOIN {s}.entries e ON e.entry_id=c.entry_id AND encode(e.state_hash,'hex')=c.state_hash",
        s = quote(schema)
    );
    // A hostname is shared by consecutive attempts. Each claim needs its own token.
    let owner = Uuid::new_v4().to_string();
    let row = transaction.query_opt(&sql, &[&owner]).await?;
    let payload = match &row {
        Some(row) => match spool_claim(&transaction, schema, row, &owner, false).await {
            Ok(payload) => Some(payload),
            Err(error) => {
                transaction.commit().await?;
                return Err(error);
            }
        },
        None => None,
    };
    transaction.commit().await?;
    Ok(row.map(|row| Intent {
        schema: schema.into(),
        entry_id: row.get(0),
        state_hash: row.get(1),
        lexical_profile_hash: row.get(2),
        submission_id: row.get(3),
        media_type: row.get(4),
        payload: payload.expect("claimed row has a spool"),
        lease_owner: owner,
        event: None,
    }))
}

/// An explicit event-v2 source assignment is required; legacy timeline-derived
/// source generations are never silently relabelled to workspace incarnation 1.
async fn journal_scope(
    transaction: &tokio_postgres::Transaction<'_>,
    schema: &str,
) -> Result<Option<(i64, i64)>> {
    let row = transaction
        .query_one(
            &format!(
                "SELECT incarnation,state FROM {}.lbfs_runtime_fence WHERE singleton FOR SHARE",
                quote(schema)
            ),
            &[],
        )
        .await?;
    let incarnation: i64 = row.get(0);
    let state: String = row.get(1);
    if !matches!(state.as_str(), "RUNNING" | "QUIESCING") {
        return Ok(None);
    }
    let config = transaction
        .query_one(
            &format!(
                "SELECT source_config_version FROM {}.lbfs_export_config WHERE singleton FOR SHARE",
                quote(schema)
            ),
            &[],
        )
        .await?;
    Ok(Some((incarnation, config.get(0))))
}

async fn claim_journal(
    client: &mut tokio_postgres::Client,
    schema: &str,
    config: &Config,
) -> Result<Option<Intent>> {
    let transaction = materialization_transaction(client).await?;
    let Some((incarnation, version)) = journal_scope(&transaction, schema).await? else {
        return Ok(None);
    };
    if incarnation as u64 != config.source_generation
        || version as u64 != config.source_config_version
    {
        bail!("export assignment no longer matches current workspace/source configuration");
    }
    let s = quote(schema);
    // Old-world events are retained for explicit retirement. Only supersede current
    // file work after its referenced revision is no longer the current entry.
    transaction.execute(&format!("UPDATE {s}.index_export_intents i SET status='dead',lease_token=NULL,lease_expires_at=NULL,last_error='source revision superseded',updated_at=clock_timestamp() WHERE i.incarnation=$1 AND i.source_config_version=$2 AND i.job_kind IN ('file_content','repair') AND i.status IN ('pending','uploading') AND NOT EXISTS(SELECT 1 FROM {s}.entries e WHERE e.entry_id=i.entry_id AND e.state_hash=i.source_state_hash AND e.content_hash=i.content_hash)"),&[&incarnation,&version]).await?;
    transaction.execute(&format!("UPDATE {s}.index_export_intents i SET status='dead',lease_token=NULL,lease_expires_at=NULL,last_error='semantic target superseded',updated_at=clock_timestamp() \
        WHERE i.incarnation=$1 AND i.source_config_version=$2 AND i.job_kind='semantic_content' AND i.status IN ('pending','uploading') \
        AND (NOT EXISTS(SELECT 1 FROM {s}.entries e WHERE e.entry_id=i.entry_id AND e.state_hash=i.source_state_hash AND e.content_hash=i.content_hash) \
          OR NOT EXISTS(SELECT 1 FROM {s}.file_semantic_state m WHERE m.entry_id=i.entry_id AND m.embedding_profile_id=i.embedding_profile_id \
              AND m.desired_state_hash=i.source_state_hash AND m.desired_fingerprint=i.fingerprint))"), &[&incarnation,&version]).await?;
    let token = Uuid::new_v4().to_string();
    // Canonical work shares the same ordered queue and lease as raw files.
    // The semantic desired identity must still match; lexical work may lag and
    // keeps its obligation pending until the exact current sections are active.
    let sql = format!(
        "WITH candidate AS (SELECT i.event_id FROM {s}.index_export_intents i \
        JOIN {s}.entries e ON e.entry_id=i.entry_id AND e.state_hash=i.source_state_hash AND e.content_hash=i.content_hash \
        LEFT JOIN {s}.file_contents f ON f.entry_id=e.entry_id \
        LEFT JOIN {s}.file_index_state l ON l.entry_id=e.entry_id \
        WHERE i.incarnation=$1 AND i.source_config_version=$2 AND i.lexical_profile_hash IS NOT NULL \
        AND ((i.job_kind IN ('file_content','repair') AND f.entry_id IS NOT NULL) \
          OR (i.job_kind='semantic_content' AND l.active_state_hash=i.source_state_hash \
              AND l.active_fingerprint=l.desired_fingerprint AND l.desired_state_hash=i.source_state_hash \
              AND EXISTS(SELECT 1 FROM {s}.file_semantic_state m WHERE m.entry_id=i.entry_id \
                  AND m.embedding_profile_id=i.embedding_profile_id AND m.desired_state_hash=i.source_state_hash \
                  AND m.desired_fingerprint=i.fingerprint))) \
        AND i.status IN ('pending','uploading') AND i.available_at<=clock_timestamp() \
        AND (i.lease_expires_at IS NULL OR i.lease_expires_at<=clock_timestamp()) \
        ORDER BY i.available_at,i.event_id FOR UPDATE OF i SKIP LOCKED LIMIT 1), \
        claimed AS (UPDATE {s}.index_export_intents i SET status='uploading',attempts=attempts+1, \
        lease_token=$3::text::uuid,lease_expires_at=clock_timestamp()+interval '2 minutes',updated_at=clock_timestamp() \
        FROM candidate c WHERE i.event_id=c.event_id RETURNING i.*) \
        SELECT c.event_id::text,c.entry_id::text,encode(c.source_state_hash,'hex'),encode(c.fingerprint,'hex'), \
        c.incarnation,c.source_config_version,c.job_kind,encode(c.lexical_profile_hash,'hex'), \
        CASE WHEN c.job_kind='semantic_content' THEN 'application/vnd.lakeon.canonical-sections+json;version=1' ELSE c.media_type END, \
        coalesce(octet_length(f.data),0),encode(c.content_hash,'hex'),encode(l.active_fingerprint,'hex') \
        FROM claimed c LEFT JOIN {s}.file_contents f ON f.entry_id=c.entry_id \
        LEFT JOIN {s}.file_index_state l ON l.entry_id=c.entry_id"
    );
    let row = transaction
        .query_opt(&sql, &[&incarnation, &version, &token])
        .await?;
    let payload = match &row {
        Some(row) => match spool_claim(&transaction, schema, row, &token, true).await {
            Ok(payload) => Some(payload),
            Err(error) => {
                transaction.commit().await?;
                return Err(error);
            }
        },
        None => None,
    };
    transaction.commit().await?;
    Ok(row.map(|row| {
        let event_id: String = row.get(0);
        Intent {
            schema: schema.into(),
            entry_id: row.get(1),
            state_hash: row.get(2),
            lexical_profile_hash: row.get(7),
            submission_id: event_id.clone(),
            media_type: row.get(8),
            payload: payload.expect("claimed event has a spool"),
            lease_owner: token,
            event: Some(EventIdentity {
                event_id,
                fingerprint: row.get(3),
                incarnation: row.get(4),
                source_config_version: row.get(5),
                job_kind: row.get(6),
                content_hash: row.get(10),
            }),
        }
    }))
}

fn source_id(config: &Config, intent: &Intent) -> Uuid {
    if let Some(event) = &intent.event {
        deterministic_source_id(
            &config.tenant_id,
            &format!("{}:incarnation:{}", intent.schema, event.incarnation),
        )
    } else {
        deterministic_source_id(&config.tenant_id, &intent.schema)
    }
}

async fn acknowledge_journal(
    client: &mut tokio_postgres::Client,
    intent: &Intent,
    key: &str,
    version: &str,
    checksum: &str,
) -> Result<()> {
    let event = intent.event.as_ref().context("event identity required")?;
    let transaction = client.transaction().await?;
    if journal_scope(&transaction, &intent.schema).await?
        != Some((event.incarnation, event.source_config_version))
    {
        bail!("export acknowledgement belongs to an old workspace/source");
    }
    let sql = format!(
        "UPDATE {}.index_export_intents SET status='exported',object_key=$1,object_version_id=$2,object_checksum=$3,object_size=$4,last_error=NULL,updated_at=clock_timestamp() WHERE event_id=$5::text::uuid AND entry_id=$6::text::uuid AND source_state_hash=decode($7,'hex') AND fingerprint=decode($8,'hex') AND incarnation=$9 AND source_config_version=$10 AND job_kind=$11 AND lease_token=$12::text::uuid AND ((status='uploading' AND lease_expires_at>clock_timestamp()) OR (status='exported' AND object_key=$1 AND object_version_id=$2 AND object_checksum=$3 AND object_size=$4))",
        quote(&intent.schema)
    );
    let size = intent.payload.len as i64;
    let changed = transaction
        .execute(
            &sql,
            &[
                &key,
                &version,
                &checksum,
                &size,
                &event.event_id,
                &intent.entry_id,
                &intent.state_hash,
                &event.fingerprint,
                &event.incarnation,
                &event.source_config_version,
                &event.job_kind,
                &intent.lease_owner,
            ],
        )
        .await?;
    if changed != 1 {
        bail!("export acknowledgement lost its event lease");
    }
    transaction.commit().await?;
    Ok(())
}

async fn retry_journal(
    client: &mut tokio_postgres::Client,
    intent: &Intent,
    error: &str,
) -> Result<()> {
    let event = intent.event.as_ref().context("event identity required")?;
    let transaction = client.transaction().await?;
    if journal_scope(&transaction, &intent.schema).await?
        != Some((event.incarnation, event.source_config_version))
    {
        return Ok(());
    }
    let sql = format!(
        "UPDATE {}.index_export_intents SET status='pending',available_at=clock_timestamp()+interval '10 seconds',lease_token=NULL,lease_expires_at=NULL,last_error=$1,updated_at=clock_timestamp() WHERE event_id=$2::text::uuid AND entry_id=$3::text::uuid AND source_state_hash=decode($4,'hex') AND fingerprint=decode($5,'hex') AND incarnation=$6 AND source_config_version=$7 AND job_kind=$8 AND status='uploading' AND lease_token=$9::text::uuid AND lease_expires_at>clock_timestamp()",
        quote(&intent.schema)
    );
    transaction
        .execute(
            &sql,
            &[
                &error.chars().take(1000).collect::<String>(),
                &event.event_id,
                &intent.entry_id,
                &intent.state_hash,
                &event.fingerprint,
                &event.incarnation,
                &event.source_config_version,
                &event.job_kind,
                &intent.lease_owner,
            ],
        )
        .await?;
    transaction.commit().await?;
    Ok(())
}

fn input_kind(intent: &Intent) -> &'static str {
    if intent
        .event
        .as_ref()
        .is_some_and(|event| event.job_kind == "semantic_content")
    {
        "canonical_sections"
    } else {
        "raw_file"
    }
}

async fn publish(
    s3: &aws_sdk_s3::Client,
    config: &Config,
    intent: &Intent,
) -> Result<(String, String, String)> {
    let checksum = intent.payload.checksum.clone();
    if let Some(event) = &intent.event {
        if matches!(event.job_kind.as_str(), "file_content" | "repair")
            && event.content_hash != checksum
        {
            bail!("source content checksum does not match committed export intent");
        }
    }
    let source_id = source_id(config, intent);
    let key = object_key(config, intent, source_id);
    let mut metadata = HashMap::new();
    metadata.insert("x-index-contract".into(), "document-object/v1".into());
    metadata.insert("x-index-input-kind".into(), input_kind(intent).into());
    metadata.insert("x-index-media-type".into(), intent.media_type.clone());
    metadata.insert("x-index-payload-sha256".into(), checksum.clone());
    let encoded_revision = segment(intent.state_hash.clone());
    metadata.insert("x-index-source-revision".into(), encoded_revision.clone());
    metadata.insert(
        "x-index-lexical-profile-hash".into(),
        intent.lexical_profile_hash.clone(),
    );
    metadata.insert("x-index-created-at".into(), created_at());
    // Reconcile a completed PUT whose ACK/response was lost. A deterministic key
    // is immutable: a conflicting object is evidence to retain, never overwrite.
    if let Some(version) = existing_version(s3, config, &key).await? {
        verify_version(s3, config, intent, &key, &version, &checksum).await?;
        return Ok((key, version, checksum));
    }
    let response = s3
        .put_object()
        .bucket(&config.bucket)
        .key(&key)
        .if_none_match("*")
        .checksum_sha256(base64::engine::general_purpose::STANDARD.encode(hex::decode(&checksum)?))
        .set_metadata(Some(metadata))
        .content_type(&intent.media_type)
        .body(intent.payload.body().await?)
        .send()
        .await;
    let version = match response {
        Ok(response) => response.version_id().unwrap_or_default().to_string(),
        Err(error) => {
            // Includes a concurrent winning PUT and an ambiguous connection loss
            // after the server committed. All other HEAD errors fail closed.
            if let Some(version) = existing_version(s3, config, &key).await? {
                verify_version(s3, config, intent, &key, &version, &checksum).await?;
                return Ok((key, version, checksum));
            }
            return Err(error.into());
        }
    };
    verify_version(s3, config, intent, &key, &version, &checksum).await?;
    Ok((key, version, checksum))
}

async fn existing_version(
    s3: &aws_sdk_s3::Client,
    config: &Config,
    key: &str,
) -> Result<Option<String>> {
    match s3
        .head_object()
        .bucket(&config.bucket)
        .key(key)
        .send()
        .await
    {
        Ok(head) => Ok(Some(head.version_id().unwrap_or_default().to_string())),
        Err(error)
            if error
                .as_service_error()
                .is_some_and(|service| service.is_not_found()) =>
        {
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

async fn verify_version(
    s3: &aws_sdk_s3::Client,
    config: &Config,
    intent: &Intent,
    key: &str,
    version: &str,
    checksum: &str,
) -> Result<()> {
    if version.is_empty() || version == "null" {
        bail!("OBS bucket did not return an immutable version ID");
    }
    let head = s3
        .head_object()
        .bucket(&config.bucket)
        .key(key)
        .version_id(version)
        .send()
        .await?;
    let encoded_revision = segment(intent.state_hash.clone());
    let metadata = head.metadata().context("OBS input metadata is missing")?;
    const FIELDS: [&str; 7] = [
        "x-index-contract",
        "x-index-input-kind",
        "x-index-media-type",
        "x-index-payload-sha256",
        "x-index-source-revision",
        "x-index-lexical-profile-hash",
        "x-index-created-at",
    ];
    let created_valid = metadata
        .get("x-index-created-at")
        .is_some_and(|value| chrono::DateTime::parse_from_rfc3339(value).is_ok());
    if head.version_id() != Some(version)
        || !created_valid
        || metadata
            .keys()
            .any(|key| key.starts_with("x-index-") && !FIELDS.contains(&key.as_str()))
        || metadata.get("x-index-media-type") != Some(&intent.media_type)
        || head.content_length() != Some(intent.payload.len as i64)
        || head.content_type() != Some(intent.media_type.as_str())
        || metadata.get("x-index-contract").map(String::as_str) != Some("document-object/v1")
        || metadata.get("x-index-input-kind").map(String::as_str) != Some(input_kind(intent))
        || metadata.get("x-index-payload-sha256").map(String::as_str) != Some(checksum)
        || metadata.get("x-index-source-revision") != Some(&encoded_revision)
        || metadata.get("x-index-lexical-profile-hash") != Some(&intent.lexical_profile_hash)
    {
        bail!("OBS immutable object metadata verification failed");
    }
    // Custom metadata is not proof of actual body bytes. Read the exact version,
    // bound the stream to the expected size, and independently hash it before ACK.
    let response = s3
        .get_object()
        .bucket(&config.bucket)
        .key(key)
        .version_id(version)
        .send()
        .await?;
    if response.version_id() != Some(version) {
        bail!("OBS returned a different object version");
    }
    let mut body = response.body;
    let mut hash = Sha256::new();
    let mut size = 0usize;
    while let Some(chunk) = body.next().await {
        let chunk = chunk?;
        size = size.checked_add(chunk.len()).context("OBS size overflow")?;
        if size > intent.payload.len {
            bail!("OBS exact version exceeds expected size");
        }
        hash.update(&chunk);
    }
    if size != intent.payload.len || hex::encode(hash.finalize()) != checksum {
        bail!("OBS immutable object body verification failed");
    }
    Ok(())
}

fn created_at() -> String {
    chrono::Utc::now().to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

fn object_key(config: &Config, intent: &Intent, source_id: Uuid) -> String {
    let values = [
        config.tenant_id.clone(),
        config.namespace_id.clone(),
        source_id.to_string(),
        config.source_config_version.to_string(),
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
    client: &mut tokio_postgres::Client,
    intent: &Intent,
    key: &str,
    version: &str,
    checksum: &str,
) -> Result<()> {
    if intent.event.is_some() {
        return acknowledge_journal(client, intent, key, version, checksum).await;
    }
    let sql = format!(
        "UPDATE {s}.index_export_outbox SET status='exported',object_key=$1,object_version_id=$2,object_checksum=$3,lease_owner=NULL,lease_expires_at=NULL,last_error=NULL,updated_at=now() WHERE entry_id=$4::text::uuid AND source_state_hash=decode($5,'hex') AND lexical_profile_hash=decode($6,'hex') AND submission_id=$7 AND lease_owner=$8 AND lease_expires_at>clock_timestamp() AND status='uploading'",
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
                &intent.lexical_profile_hash,
                &intent.submission_id,
                &intent.lease_owner,
            ],
        )
        .await?
        != 1
    {
        bail!("export acknowledgement lost its claim");
    }
    Ok(())
}

async fn retry(client: &mut tokio_postgres::Client, intent: &Intent, error: &str) -> Result<()> {
    if intent.event.is_some() {
        return retry_journal(client, intent, error).await;
    }
    let sql = format!(
        "UPDATE {s}.index_export_outbox SET status='pending',available_at=now()+interval '10 seconds',lease_owner=NULL,lease_expires_at=NULL,last_error=$1,updated_at=now() WHERE entry_id=$2::text::uuid AND source_state_hash=decode($3,'hex') AND lexical_profile_hash=decode($4,'hex') AND submission_id=$5 AND lease_owner=$6 AND lease_expires_at>clock_timestamp() AND status='uploading'",
        s = quote(&intent.schema)
    );
    client
        .execute(
            &sql,
            &[
                &error.chars().take(1000).collect::<String>(),
                &intent.entry_id,
                &intent.state_hash,
                &intent.lexical_profile_hash,
                &intent.submission_id,
                &intent.lease_owner,
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
#[path = "index_exporter/canonical_tests.rs"]
mod canonical_tests;

#[cfg(test)]
mod tests {
    use super::*;
    async fn test_spool(bytes: &[u8]) -> Spool {
        let mut file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        file.write_all(bytes).await.unwrap();
        file.flush().await.unwrap();
        Spool {
            file,
            len: bytes.len(),
            checksum: hex::encode(Sha256::digest(bytes)),
        }
    }

    pub(super) struct TestPostgres(pub(super) String);
    impl Drop for TestPostgres {
        fn drop(&mut self) {
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", &self.0])
                .output();
        }
    }
    pub(super) async fn postgres() -> (TestPostgres, tokio_postgres::Client) {
        let output = std::process::Command::new("docker")
            .args([
                "run",
                "--rm",
                "-d",
                "-p",
                "127.0.0.1::5432",
                "-e",
                "POSTGRES_HOST_AUTH_METHOD=trust",
                "postgres:17",
            ])
            .output()
            .expect("Docker required for actual exporter SQL races");
        assert!(
            output.status.success(),
            "Postgres test container did not start"
        );
        let owned = TestPostgres(String::from_utf8(output.stdout).unwrap().trim().to_string());
        let output = std::process::Command::new("docker")
            .args(["port", &owned.0, "5432/tcp"])
            .output()
            .unwrap();
        assert!(output.status.success());
        let address = String::from_utf8(output.stdout).unwrap();
        let port: u16 = address.trim().rsplit(':').next().unwrap().parse().unwrap();
        let mut config = tokio_postgres::Config::new();
        config
            .host("127.0.0.1")
            .port(port)
            .user("postgres")
            .dbname("postgres");
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            match config.connect(tokio_postgres::NoTls).await {
                Ok((client, connection)) => {
                    tokio::spawn(async move {
                        let _ = connection.await;
                    });
                    return (owned, client);
                }
                Err(error) if std::time::Instant::now() >= deadline => {
                    panic!("Postgres test readiness: {error}")
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    }

    async fn legacy_fixture(client: &tokio_postgres::Client) {
        legacy_fixture_schema(client, "folder_a").await;
    }

    async fn legacy_fixture_schema(client: &tokio_postgres::Client, schema: &str) {
        client.batch_execute(&"CREATE SCHEMA folder_a;
            CREATE TABLE folder_a.entries(entry_id uuid PRIMARY KEY,state_hash bytea);
            CREATE TABLE folder_a.file_contents(entry_id uuid PRIMARY KEY,data bytea);
            CREATE TABLE folder_a.index_export_outbox(entry_id uuid,source_state_hash bytea,lexical_profile_hash bytea,
                submission_id text,media_type text,status text,available_at timestamptz DEFAULT now(),lease_owner text,
                lease_expires_at timestamptz,attempts int DEFAULT 0,last_error text,updated_at timestamptz DEFAULT now(),
                object_key text,object_version_id text,object_checksum text);
            INSERT INTO folder_a.entries VALUES('00000000-0000-0000-0000-000000000001',decode(repeat('11',32),'hex'));
            INSERT INTO folder_a.file_contents VALUES('00000000-0000-0000-0000-000000000001',decode('6162','hex'));
            INSERT INTO folder_a.index_export_outbox(entry_id,source_state_hash,lexical_profile_hash,submission_id,media_type,status)
            VALUES('00000000-0000-0000-0000-000000000001',decode(repeat('11',32),'hex'),decode(repeat('22',32),'hex'),'event-a','text/markdown','pending');".replace("folder_a",schema)).await.unwrap();
    }

    #[tokio::test]
    async fn source_snapshot_survives_concurrent_same_size_replacement() {
        use std::os::unix::fs::MetadataExt;
        let (owned, mut client) = postgres().await;
        legacy_fixture(&client).await;
        client
            .execute(
                "UPDATE folder_a.file_contents SET data=convert_to(repeat('ab',1048583),'UTF8')",
                &[],
            )
            .await
            .unwrap();
        let output = std::process::Command::new("docker")
            .args(["port", &owned.0, "5432/tcp"])
            .output()
            .unwrap();
        assert!(output.status.success());
        let address = String::from_utf8(output.stdout).unwrap();
        let port: u16 = address.trim().rsplit(':').next().unwrap().parse().unwrap();
        let (writer, connection) = tokio_postgres::connect(
            &format!("host=127.0.0.1 port={port} user=postgres dbname=postgres"),
            tokio_postgres::NoTls,
        )
        .await
        .unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });
        let transaction = materialization_transaction(&mut client).await.unwrap();
        let len: i32 = transaction
            .query_one("SELECT octet_length(data) FROM folder_a.file_contents", &[])
            .await
            .unwrap()
            .get(0);
        writer
            .execute(
                "UPDATE folder_a.file_contents SET data=convert_to(repeat('cd',1048583),'UTF8')",
                &[],
            )
            .await
            .unwrap();
        let spool = spool_source(
            &transaction,
            "folder_a",
            "00000000-0000-0000-0000-000000000001",
            len,
        )
        .await
        .unwrap();
        assert_eq!(spool.len, 2097166);
        assert_eq!(
            spool.checksum,
            "1b52fa3b6401c1f3c3d16e0a8d30e4b038a84a8ae0966c955a1cc04272c00118"
        );
        let metadata = spool.file.metadata().await.unwrap();
        assert_eq!(metadata.len(), 2097166);
        assert_eq!(
            metadata.nlink(),
            0,
            "temporary plaintext must have no surviving pathname"
        );
        assert_eq!(metadata.mode() & 0o777, 0o600);
        // Reopening for a second request must start at byte zero, including empty files.
        for _ in 0..2 {
            let body = spool
                .body()
                .await
                .unwrap()
                .collect()
                .await
                .unwrap()
                .into_bytes();
            assert_eq!(body.len(), 2097166);
            assert!(body.chunks_exact(2).all(|pair| pair == b"ab"));
        }
        transaction.commit().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_materialization_leaves_the_durable_intent_pending() {
        let (owned, mut client) = postgres().await;
        legacy_fixture(&client).await;
        let output = std::process::Command::new("docker")
            .args(["port", &owned.0, "5432/tcp"])
            .output()
            .unwrap();
        assert!(output.status.success());
        let address = String::from_utf8(output.stdout).unwrap();
        let port: u16 = address.trim().rsplit(':').next().unwrap().parse().unwrap();
        let (mut blocker, connection) = tokio_postgres::connect(
            &format!("host=127.0.0.1 port={port} user=postgres dbname=postgres"),
            tokio_postgres::NoTls,
        )
        .await
        .unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });
        let locked = blocker.transaction().await.unwrap();
        locked
            .batch_execute("LOCK folder_a.file_contents IN ACCESS EXCLUSIVE MODE")
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), claim(&mut client, "folder_a"))
                .await
                .is_err()
        );
        locked.rollback().await.unwrap();
        let row = client
            .query_one(
                "SELECT status,attempts FROM folder_a.index_export_outbox",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>(0), "pending");
        assert_eq!(row.get::<_, i32>(1), 0);
        assert!(
            claim(&mut client, "folder_a").await.unwrap().is_some(),
            "next attempt must still acquire the obligation"
        );
    }

    #[tokio::test]
    async fn failed_folder_does_not_prevent_another_folder_from_exporting() {
        let (_owned, mut client) = postgres().await;
        legacy_fixture(&client).await;
        legacy_fixture_schema(&client, "folder_b").await;
        client
            .execute(
                "UPDATE folder_a.file_contents SET data=convert_to(repeat('x',67108865),'UTF8')",
                &[],
            )
            .await
            .unwrap();
        let config = Config {
            database_name: "test".into(),
            tenant_id: "test".into(),
            namespace_id: "lakebasefs".into(),
            source_generation: 1,
            source_config_version: 1,
            journal: false,
            endpoint: "test".into(),
            region: "test".into(),
            bucket: "test".into(),
        };
        let mut schemas = vec!["folder_a".into(), "folder_b".into()].into_iter();
        let healthy = next_claim(&mut client, &mut schemas, &config)
            .await
            .unwrap();
        assert_eq!(healthy.schema, "folder_b");
        acknowledge(&mut client, &healthy, "healthy-key", "v1", &"33".repeat(32))
            .await
            .unwrap();
        assert!(
            next_claim(&mut client, &mut schemas, &config)
                .await
                .is_none()
        );
        let status: String = client
            .query_one("SELECT status FROM folder_b.index_export_outbox", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(status, "exported");
    }

    #[tokio::test]
    async fn stale_materialization_backoff_cannot_reset_a_new_lease() {
        let (_owned, mut client) = postgres().await;
        legacy_fixture(&client).await;
        let first = claim(&mut client, "folder_a").await.unwrap().unwrap();
        let row=client.query_one("SELECT entry_id::text,encode(source_state_hash,'hex'),encode(lexical_profile_hash,'hex'),submission_id FROM folder_a.index_export_outbox",&[]).await.unwrap();
        client.execute("UPDATE folder_a.index_export_outbox SET lease_expires_at=clock_timestamp()-interval '1 second'",&[]).await.unwrap();
        let next = claim(&mut client, "folder_a").await.unwrap().unwrap();
        let transaction = client.transaction().await.unwrap();
        assert!(
            defer_materialization(
                &transaction,
                "folder_a",
                &row,
                &first.lease_owner,
                false,
                "old failure"
            )
            .await
            .is_err()
        );
        transaction.commit().await.unwrap();
        let row = client
            .query_one(
                "SELECT status,lease_owner,attempts FROM folder_a.index_export_outbox",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>(0), "uploading");
        assert_eq!(row.get::<_, String>(1), next.lease_owner);
        assert_eq!(row.get::<_, i32>(2), 2);
        acknowledge(&mut client, &next, "next-key", "v1", &"33".repeat(32))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn failed_materialization_does_not_starve_the_next_file() {
        let (_owned, mut client) = postgres().await;
        legacy_fixture(&client).await;
        client.batch_execute("UPDATE folder_a.file_contents SET data=convert_to(repeat('x',67108865),'UTF8');
            INSERT INTO folder_a.entries VALUES('00000000-0000-0000-0000-000000000002',decode(repeat('22',32),'hex'));
            INSERT INTO folder_a.file_contents VALUES('00000000-0000-0000-0000-000000000002',decode('6162','hex'));
            INSERT INTO folder_a.index_export_outbox(entry_id,source_state_hash,lexical_profile_hash,submission_id,media_type,status)
            VALUES('00000000-0000-0000-0000-000000000002',decode(repeat('22',32),'hex'),decode(repeat('33',32),'hex'),'event-b','text/markdown','pending');").await.unwrap();
        assert!(claim(&mut client, "folder_a").await.is_err());
        let next = claim(&mut client, "folder_a")
            .await
            .expect("failed head must back off so the next file can proceed")
            .unwrap();
        assert_eq!(next.entry_id, "00000000-0000-0000-0000-000000000002");
        acknowledge(&mut client, &next, "next-key", "v1", &"33".repeat(32))
            .await
            .unwrap();
        let row = client.query_one("SELECT status,attempts,available_at>clock_timestamp(),lease_owner FROM folder_a.index_export_outbox WHERE submission_id='event-a'",&[]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "pending");
        assert_eq!(row.get::<_, i32>(1), 1);
        assert!(row.get::<_, bool>(2));
        assert_eq!(row.get::<_, Option<String>>(3), None);
    }

    #[tokio::test]
    async fn oversized_file_is_not_materialized_or_leased() {
        let (_owned, mut client) = postgres().await;
        legacy_fixture(&client).await;
        client
            .execute(
                "UPDATE folder_a.file_contents SET data=convert_to(repeat('x',67108865),'UTF8')",
                &[],
            )
            .await
            .unwrap();
        assert!(
            claim(&mut client, "folder_a").await.is_err(),
            "source beyond LakebaseFS 64MiB schema contract must fail before spooling"
        );
        let row = client
            .query_one(
                "SELECT status,attempts FROM folder_a.index_export_outbox",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>(0), "pending");
        assert_eq!(
            row.get::<_, i32>(1),
            1,
            "failed materialization must persist a guarded backoff"
        );
    }

    #[tokio::test]
    async fn late_legacy_ack_and_retry_cannot_modify_a_reclaimed_lease() {
        let (_owned, mut client) = postgres().await;
        legacy_fixture(&client).await;
        let first = claim(&mut client, "folder_a").await.unwrap().unwrap();
        client.execute("UPDATE folder_a.index_export_outbox SET lease_expires_at=now()-interval '1 second'", &[]).await.unwrap();
        let next = claim(&mut client, "folder_a").await.unwrap().unwrap();
        assert!(
            acknowledge(&mut client, &first, "key", "old-version", &"33".repeat(32))
                .await
                .is_err(),
            "old process must not ACK another upload attempt"
        );
        retry(&mut client, &first, "old process error")
            .await
            .unwrap();
        assert!(
            acknowledge(&mut client, &next, "key", "new-version", &"33".repeat(32))
                .await
                .is_ok(),
            "late retry must not reset a reclaimed live attempt"
        );
    }

    pub(super) async fn journal_fixture(client: &tokio_postgres::Client) {
        client
            .batch_execute("CREATE SCHEMA folder_v2; SET search_path=folder_v2,public;")
            .await
            .unwrap();
        client
            .batch_execute(include_str!("index_export_event_v2.sql"))
            .await
            .unwrap();
        client.batch_execute("CREATE TABLE lbfs_runtime_fence(singleton boolean PRIMARY KEY,incarnation bigint,state text);
            INSERT INTO lbfs_runtime_fence VALUES(true,1,'RUNNING');
            CREATE TABLE entries(entry_id uuid PRIMARY KEY,state_hash bytea,content_hash bytea);
            CREATE TABLE file_contents(entry_id uuid PRIMARY KEY,data bytea);
            CREATE TABLE file_index_state(entry_id uuid PRIMARY KEY,desired_state_hash bytea,
                desired_fingerprint bytea,active_state_hash bytea,active_fingerprint bytea);
            CREATE TABLE file_semantic_state(entry_id uuid,embedding_profile_id text,
                desired_state_hash bytea,desired_fingerprint bytea,PRIMARY KEY(entry_id,embedding_profile_id));
            INSERT INTO entries VALUES('00000000-0000-0000-0000-000000000001',decode(repeat('11',32),'hex'),decode(repeat('44',32),'hex'));
            INSERT INTO file_contents VALUES('00000000-0000-0000-0000-000000000001',decode('6162','hex'));
            INSERT INTO index_export_intents(event_id,entry_id,source_state_hash,fingerprint,incarnation,source_config_version,job_kind,content_hash,lexical_profile_hash,media_type)
            VALUES('00000000-0000-0000-0000-000000000011','00000000-0000-0000-0000-000000000001',decode(repeat('11',32),'hex'),decode(repeat('22',32),'hex'),1,1,'file_content',decode(repeat('44',32),'hex'),decode(repeat('33',32),'hex'),'text/markdown');").await.unwrap();
    }

    #[tokio::test]
    async fn journal_oversize_rolls_back_and_empty_file_remains_exportable() {
        let (_owned, mut client) = postgres().await;
        journal_fixture(&client).await;
        let config = Config {
            database_name: "test".into(),
            tenant_id: "test".into(),
            namespace_id: "lakebasefs".into(),
            source_generation: 1,
            source_config_version: 1,
            journal: true,
            endpoint: "test".into(),
            region: "test".into(),
            bucket: "test".into(),
        };
        client
            .execute(
                "UPDATE file_contents SET data=convert_to(repeat('x',67108865),'UTF8')",
                &[],
            )
            .await
            .unwrap();
        assert!(
            claim_journal(&mut client, "folder_v2", &config)
                .await
                .is_err()
        );
        let row = client
            .query_one("SELECT status,attempts FROM index_export_intents", &[])
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>(0), "pending");
        assert_eq!(row.get::<_, i32>(1), 1);
        client
            .execute("UPDATE file_contents SET data=''::bytea", &[])
            .await
            .unwrap();
        client.execute("UPDATE index_export_intents SET available_at=clock_timestamp()-interval '1 second'",&[]).await.unwrap();
        let empty = claim_journal(&mut client, "folder_v2", &config)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(empty.payload.len, 0);
        assert_eq!(
            empty.payload.checksum,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert!(
            empty
                .payload
                .body()
                .await
                .unwrap()
                .collect()
                .await
                .unwrap()
                .into_bytes()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn schema_limit_file_is_exportable_without_a_whole_body_buffer() {
        let (_owned, mut client) = postgres().await;
        legacy_fixture(&client).await;
        client
            .execute(
                "UPDATE folder_a.file_contents SET data=convert_to(repeat('x',67108864),'UTF8')",
                &[],
            )
            .await
            .unwrap();
        let intent = tokio::time::timeout(Duration::from_secs(30), claim(&mut client, "folder_a"))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(intent.payload.len, 67108864);
        assert_eq!(
            intent.payload.file.metadata().await.unwrap().len(),
            67108864
        );
        assert_eq!(
            intent.payload.checksum,
            "e20a69eca39368572e90b9135738a613838f954987a0b44b6220889c171cbb76"
        );
    }

    #[tokio::test]
    async fn journal_full_identity_lease_and_restore_fence_are_enforced_by_actual_postgres() {
        let (_owned, mut client) = postgres().await;
        journal_fixture(&client).await;
        let mut config = Config {
            database_name: "test".into(),
            tenant_id: "test-tenant".into(),
            namespace_id: "lakebasefs".into(),
            source_generation: 1,
            source_config_version: 1,
            journal: true,
            endpoint: "test".into(),
            region: "test".into(),
            bucket: "test".into(),
        };
        let first = claim_journal(&mut client, "folder_v2", &config)
            .await
            .unwrap()
            .unwrap();
        client
            .execute(
                "UPDATE index_export_intents SET lease_expires_at=now()-interval '1 second'",
                &[],
            )
            .await
            .unwrap();
        let next = claim_journal(&mut client, "folder_v2", &config)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(first.lease_owner, next.lease_owner);
        assert!(
            acknowledge(&mut client, &first, "key", "old-version", &"55".repeat(32))
                .await
                .is_err()
        );
        retry(&mut client, &first, "late failure").await.unwrap();
        acknowledge(&mut client, &next, "key", "new-version", &"55".repeat(32))
            .await
            .unwrap();
        acknowledge(&mut client, &next, "key", "new-version", &"55".repeat(32))
            .await
            .unwrap();
        assert!(
            acknowledge(
                &mut client,
                &next,
                "key",
                "conflicting-version",
                &"55".repeat(32)
            )
            .await
            .is_err()
        );
        let mut forged = next;
        forged.event.as_mut().unwrap().fingerprint = "66".repeat(32);
        assert!(
            acknowledge(&mut client, &forged, "key", "new-version", &"55".repeat(32))
                .await
                .is_err()
        );
        forged.event.as_mut().unwrap().fingerprint = "22".repeat(32);
        let old_source = source_id(&config, &forged);
        let old_key = object_key(&config, &forged, old_source);
        client
            .execute("UPDATE lbfs_runtime_fence SET incarnation=2", &[])
            .await
            .unwrap();
        assert!(
            acknowledge(&mut client, &forged, "key", "new-version", &"55".repeat(32))
                .await
                .is_err()
        );
        assert!(
            claim_journal(&mut client, "folder_v2", &config)
                .await
                .is_err(),
            "never silently upgrade an old source assignment"
        );
        config.source_generation = 2;
        client.batch_execute("INSERT INTO index_export_intents(event_id,entry_id,source_state_hash,fingerprint,incarnation,source_config_version,job_kind,content_hash,lexical_profile_hash,media_type)
            VALUES('00000000-0000-0000-0000-000000000012','00000000-0000-0000-0000-000000000001',decode(repeat('11',32),'hex'),decode(repeat('22',32),'hex'),2,1,'file_content',decode(repeat('44',32),'hex'),decode(repeat('33',32),'hex'),'text/markdown');").await.unwrap();
        let restored = claim_journal(&mut client, "folder_v2", &config)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(source_id(&config, &restored), old_source);
        assert_ne!(
            object_key(&config, &restored, source_id(&config, &restored)),
            old_key
        );
        client
            .execute("UPDATE lbfs_export_config SET source_config_version=2", &[])
            .await
            .unwrap();
        assert!(
            acknowledge(
                &mut client,
                &restored,
                "key",
                "restored-version",
                &"55".repeat(32)
            )
            .await
            .is_err()
        );
        assert!(
            claim_journal(&mut client, "folder_v2", &config)
                .await
                .is_err()
        );
    }

    pub(super) async fn minio() -> (TestPostgres, aws_sdk_s3::Client, Config) {
        let output = std::process::Command::new("docker")
            .args([
                "run",
                "--rm",
                "-d",
                "-p",
                "127.0.0.1::9000",
                "-e",
                "MINIO_ROOT_USER=minioadmin",
                "-e",
                "MINIO_ROOT_PASSWORD=minioadmin",
                "lakeon-test/minio:20241107-cefc43e4daa4",
                "server",
                "/data",
            ])
            .output()
            .expect("Docker required for actual versioned object handoff");
        assert!(
            output.status.success(),
            "MinIO test container did not start"
        );
        let owned = TestPostgres(String::from_utf8(output.stdout).unwrap().trim().to_string());
        let output = std::process::Command::new("docker")
            .args(["port", &owned.0, "9000/tcp"])
            .output()
            .unwrap();
        assert!(output.status.success());
        let address = String::from_utf8(output.stdout).unwrap();
        let port: u16 = address.trim().rsplit(':').next().unwrap().parse().unwrap();
        let config = Config {
            database_name: "test".into(),
            tenant_id: "test-tenant".into(),
            namespace_id: "lakebasefs".into(),
            source_generation: 1,
            source_config_version: 1,
            journal: false,
            endpoint: format!("http://127.0.0.1:{port}"),
            region: "us-east-1".into(),
            bucket: "lbfs-export-test".into(),
        };
        let sdk = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::v2024_03_28())
            .region(Region::new(config.region.clone()))
            .endpoint_url(&config.endpoint)
            .force_path_style(true)
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "minioadmin",
                "minioadmin",
                None,
                None,
                "isolated-test",
            ))
            .build();
        let s3 = aws_sdk_s3::Client::from_conf(sdk);
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            if s3
                .create_bucket()
                .bucket(&config.bucket)
                .send()
                .await
                .is_ok()
            {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "MinIO test readiness");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        s3.put_bucket_versioning()
            .bucket(&config.bucket)
            .versioning_configuration(
                aws_sdk_s3::types::VersioningConfiguration::builder()
                    .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await
            .unwrap();
        (owned, s3, config)
    }

    #[tokio::test]
    async fn lost_ack_reuses_exact_version_and_same_key_corruption_is_not_overwritten() {
        let (_owned, s3, config) = minio().await;
        let intent = Intent {
            schema: "folder_a".into(),
            entry_id: Uuid::nil().to_string(),
            state_hash: "11".repeat(32),
            lexical_profile_hash: "22".repeat(32),
            submission_id: "immutable-event-1".into(),
            media_type: "text/markdown".into(),
            payload: test_spool(b"hello").await,
            lease_owner: Uuid::new_v4().to_string(),
            event: None,
        };
        let first = publish(&s3, &config, &intent).await.unwrap();
        let replay = publish(&s3, &config, &intent).await.unwrap();
        assert_eq!(
            first, replay,
            "lost ACK must reconcile the existing immutable version rather than PUT another one"
        );
        let head = s3
            .head_object()
            .bucket(&config.bucket)
            .key(&first.0)
            .version_id(&first.1)
            .send()
            .await
            .unwrap();
        for field in [
            "x-index-media-type",
            "x-index-created-at",
            "x-index-unrecognized",
        ] {
            let mut metadata = head.metadata().cloned().unwrap();
            metadata.insert(field.into(), "invalid-contract-value".into());
            s3.put_object()
                .bucket(&config.bucket)
                .key(&first.0)
                .set_metadata(Some(metadata))
                .content_type(&intent.media_type)
                .body(ByteStream::from_static(b"hello"))
                .send()
                .await
                .unwrap();
            assert!(
                publish(&s3, &config, &intent).await.is_err(),
                "contract metadata mismatch must not be ACKed: {field}"
            );
        }
        let corrupt = s3
            .put_object()
            .bucket(&config.bucket)
            .key(&first.0)
            .set_metadata(head.metadata().cloned())
            .content_type(&intent.media_type)
            .body(ByteStream::from_static(b"xxxxx"))
            .send()
            .await
            .unwrap();
        let corrupt_version = corrupt.version_id().unwrap();
        assert!(
            publish(&s3, &config, &intent).await.is_err(),
            "metadata checksum alone is not evidence of actual bytes"
        );
        let latest = s3
            .head_object()
            .bucket(&config.bucket)
            .key(&first.0)
            .send()
            .await
            .unwrap();
        assert_eq!(
            latest.version_id(),
            Some(corrupt_version),
            "a conflicting deterministic key must not be overwritten"
        );
        let old = s3
            .get_object()
            .bucket(&config.bucket)
            .key(&first.0)
            .version_id(&first.1)
            .send()
            .await
            .unwrap();
        assert_eq!(
            old.body.collect().await.unwrap().into_bytes().as_ref(),
            b"hello"
        );
        // Real PG -> multiple source chunks -> anonymous spool -> versioned
        // object readback -> exact lease ACK, without a test-only producer.
        let (_pg, mut client) = postgres().await;
        legacy_fixture(&client).await;
        client
            .execute(
                "UPDATE folder_a.file_contents SET data=convert_to(repeat('ab',1048583),'UTF8')",
                &[],
            )
            .await
            .unwrap();
        let large = claim(&mut client, "folder_a").await.unwrap().unwrap();
        assert_eq!(large.payload.len, 2097166);
        let uploaded = publish(&s3, &config, &large).await.unwrap();
        assert_eq!(
            uploaded.2,
            "1b52fa3b6401c1f3c3d16e0a8d30e4b038a84a8ae0966c955a1cc04272c00118"
        );
        assert_eq!(
            publish(&s3, &config, &large).await.unwrap(),
            uploaded,
            "lost ACK must reuse the exact multi-chunk version"
        );
        acknowledge(&mut client, &large, &uploaded.0, &uploaded.1, &uploaded.2)
            .await
            .unwrap();
        let receipt = client
            .query_one(
                "SELECT status,object_version_id,object_checksum FROM folder_a.index_export_outbox",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(receipt.get::<_, String>(0), "exported");
        assert_eq!(receipt.get::<_, String>(1), uploaded.1);
        assert_eq!(receipt.get::<_, String>(2), uploaded.2);

        let malformed = Intent {
            submission_id: "bad-source-checksum".into(),
            event: Some(EventIdentity {
                event_id: Uuid::new_v4().to_string(),
                fingerprint: "22".repeat(32),
                incarnation: 1,
                source_config_version: 1,
                job_kind: "file_content".into(),
                content_hash: "00".repeat(32),
            }),
            ..intent
        };
        assert!(
            publish(&s3, &config, &malformed).await.is_err(),
            "exported body must match the committed source content hash"
        );
    }

    #[test]
    fn created_at_uses_utc_z_suffix_required_by_index_contract() {
        let value = created_at();
        assert!(value.ends_with('Z'), "unexpected created-at: {value}");
        assert!(chrono::DateTime::parse_from_rfc3339(&value).is_ok());
    }

    #[tokio::test]
    async fn source_identity_and_object_key_are_stable() {
        let source = deterministic_source_id("tenant-a", "folder_rf_a");
        assert_eq!(source.to_string(), "c3e9b970-29df-506e-ab86-6f7007629879");
        assert_eq!(source, deterministic_source_id("tenant-a", "folder_rf_a"));
        assert_ne!(source, deterministic_source_id("tenant-a", "folder_rf_b"));
        let config = Config {
            database_name: "x".into(),
            tenant_id: "tenant-a".into(),
            namespace_id: "lakebasefs".into(),
            source_generation: 7,
            source_config_version: 1,
            journal: false,
            endpoint: "x".into(),
            region: "x".into(),
            bucket: "x".into(),
        };
        let intent = Intent {
            schema: "folder_rf_a".into(),
            entry_id: Uuid::nil().to_string(),
            state_hash: "11".repeat(32),
            lexical_profile_hash: "22".repeat(32),
            submission_id: "event:1".into(),
            media_type: "text/markdown".into(),
            payload: test_spool(b"").await,
            lease_owner: Uuid::nil().to_string(),
            event: None,
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
