use super::tests::{journal_fixture, postgres};
use super::*;

fn config() -> Config {
    Config {
        database_name: "test".into(),
        tenant_id: "test".into(),
        namespace_id: "lakebasefs".into(),
        source_generation: 1,
        source_config_version: 1,
        journal: true,
        endpoint: "test".into(),
        region: "test".into(),
        bucket: "test".into(),
    }
}

async fn fixture(client: &tokio_postgres::Client) {
    journal_fixture(client).await;
    client.batch_execute(r#"
        DELETE FROM index_export_intents;
        CREATE TABLE section_occurrences(entry_id uuid,ordinal int,source_state_hash bytea,
            section_content_hash bytea,line_start int,line_end int,PRIMARY KEY(entry_id,ordinal));
        CREATE TABLE section_contents(section_content_hash bytea,lexical_profile_hash bytea,
            heading text,body text,PRIMARY KEY(section_content_hash,lexical_profile_hash));
        INSERT INTO file_index_state SELECT entry_id,state_hash,decode(repeat('55',32),'hex'),
            state_hash,decode(repeat('55',32),'hex') FROM entries;
        INSERT INTO file_semantic_state SELECT entry_id,'embed@1',state_hash,decode(repeat('77',32),'hex') FROM entries;
        INSERT INTO section_contents VALUES
            (decode('0ef7cc6214138de19a230af7c4f26b6a83e032f55b5b01bb652aed9fe381ef5d','hex'),decode(repeat('33',32),'hex'),
            'Notes',$text$Hello "world"
雪\end$text$),
            (decode('a94dd4d3c2c6d2548ca4e560d72727bab5d795500191f5b85579130dd3b14603','hex'),decode(repeat('33',32),'hex'),NULL,'second'),
            (decode('0ef7cc6214138de19a230af7c4f26b6a83e032f55b5b01bb652aed9fe381ef5d','hex'),decode(repeat('66',32),'hex'),'Wrong profile','wrong text');
        INSERT INTO section_occurrences SELECT entry_id,0,state_hash,
            decode('0ef7cc6214138de19a230af7c4f26b6a83e032f55b5b01bb652aed9fe381ef5d','hex'),1,2 FROM entries;
        INSERT INTO section_occurrences SELECT entry_id,1,state_hash,
            decode('a94dd4d3c2c6d2548ca4e560d72727bab5d795500191f5b85579130dd3b14603','hex'),3,3 FROM entries;
        INSERT INTO index_export_intents(event_id,entry_id,source_state_hash,fingerprint,incarnation,
            source_config_version,job_kind,content_hash,lexical_profile_hash,embedding_profile_id,media_type)
            SELECT '00000000-0000-0000-0000-000000000012',entry_id,state_hash,decode(repeat('77',32),'hex'),
                1,1,'semantic_content',content_hash,decode(repeat('33',32),'hex'),'embed@1','text/markdown' FROM entries;
        "#).await.unwrap();
}

// Matches LakebaseFSSectionIdentity.contentHash, including UTF-8 byte lengths.
fn native_section_hash(heading: &str, body: &str) -> String {
    let mut hash = Sha256::new();
    for value in ["lbfs:section:v1", heading, body] {
        hash.update((value.len() as i32).to_be_bytes());
        hash.update(value.as_bytes());
    }
    hex::encode(hash.finalize())
}

async fn native_fixture(client: &tokio_postgres::Client) -> Vec<String> {
    let mut expected = Vec::new();
    for (ordinal, heading, body) in [
        (0i32, "Notes", "Hello \"world\"\n雪\\end"),
        (1, "", "second"),
    ] {
        let hash = native_section_hash(heading, body);
        client.execute("UPDATE section_contents SET section_content_hash=decode($1,'hex') WHERE lexical_profile_hash=decode(repeat('33',32),'hex') AND coalesce(heading,'')=$2", &[&hash,&heading]).await.unwrap();
        client.execute("UPDATE section_occurrences SET section_content_hash=decode($1,'hex') WHERE ordinal=$2", &[&hash,&ordinal]).await.unwrap();
        expected.push(hash);
    }
    expected
}

#[tokio::test]
async fn zero_occurrences_without_an_empty_current_source_preserve_pending_obligation() {
    for mutation in [
        "",
        "DELETE FROM file_contents",
        "UPDATE file_contents SET data=''::bytea",
    ] {
        let (_pg, mut client) = postgres().await;
        fixture(&client).await;
        client
            .batch_execute("DELETE FROM section_occurrences")
            .await
            .unwrap();
        client.batch_execute(mutation).await.unwrap();
        let error = claim_journal(&mut client, "folder_v2", &config())
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("proven empty current file"),
            "{error:#}"
        );
        let row = client.query_one("SELECT status,object_version_id,lease_token::text,available_at>clock_timestamp() FROM index_export_intents",&[]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "pending");
        assert!(row.get::<_, Option<String>>(1).is_none());
        assert!(row.get::<_, Option<String>>(2).is_none());
        assert!(row.get::<_, bool>(3));
    }
}

#[tokio::test]
async fn empty_file_canonical_exports_exact_version_and_acks_without_a_fake_section() {
    let (_pg, mut client) = postgres().await;
    fixture(&client).await;
    client.batch_execute("DELETE FROM section_occurrences; UPDATE file_contents SET data=''::bytea; UPDATE entries SET content_hash=sha256(''::bytea); DELETE FROM index_export_intents; INSERT INTO index_export_intents(event_id,entry_id,source_state_hash,fingerprint,incarnation,source_config_version,job_kind,content_hash,lexical_profile_hash,embedding_profile_id,media_type) SELECT '00000000-0000-0000-0000-000000000042',entry_id,state_hash,decode(repeat('77',32),'hex'),1,1,'semantic_content',content_hash,decode(repeat('33',32),'hex'),'embed@1','text/markdown' FROM entries;").await.unwrap();
    let (_obs, s3, mut config) = super::tests::minio().await;
    config.journal = true;
    let intent = claim_journal(&mut client, "folder_v2", &config)
        .await
        .unwrap()
        .expect("a proven empty current lexical snapshot must be deliverable");
    let receipt = publish(&s3, &config, &intent).await.unwrap();
    let bytes = s3
        .get_object()
        .bucket(&config.bucket)
        .key(&receipt.0)
        .version_id(&receipt.1)
        .send()
        .await
        .unwrap()
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["sections"], serde_json::json!([]));
    assert_eq!(json["source_state_hash"], "11".repeat(32));
    assert_eq!(json["lexical_fingerprint"], "55".repeat(32));
    assert_eq!(publish(&s3, &config, &intent).await.unwrap(), receipt);
    acknowledge(&mut client, &intent, &receipt.0, &receipt.1, &receipt.2)
        .await
        .unwrap();
    assert_eq!(
        client
            .query_one("SELECT status FROM index_export_intents", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "exported"
    );
    if let Ok(path) = std::env::var("LBFS_EXPORT_EMPTY_CANONICAL_PROOF_PATH") {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .unwrap();
        output.write_all(&bytes).unwrap();
    }
}

#[tokio::test]
async fn semantic_exports_native_parser_section_identities_without_rewriting_them() {
    let (_owned, mut client) = postgres().await;
    fixture(&client).await;
    let expected = native_fixture(&client).await;
    let intent = claim_journal(&mut client, "folder_v2", &config())
        .await
        .unwrap()
        .expect("valid native-parser sections must export instead of backing off");
    let bytes = intent
        .payload
        .body()
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_bytes();
    let actual: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    for (section, hash) in actual["sections"].as_array().unwrap().iter().zip(expected) {
        assert_eq!(section["section_content_hash"], hash);
    }
    assert_eq!(actual["sections"][0]["body"], "Hello \"world\"\n雪\\end");
}

#[tokio::test]
async fn semantic_only_exports_current_sections_with_exact_lexical_profile() {
    let (_owned, mut client) = postgres().await;
    fixture(&client).await;
    let intent = claim_journal(&mut client, "folder_v2", &config())
        .await
        .unwrap()
        .expect("current semantic obligation must produce a canonical input");
    let bytes = intent
        .payload
        .body()
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_bytes();
    let actual: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        actual,
        serde_json::json!({
            "schema":"canonical-sections/v1", "source_state_hash":"11".repeat(32),
            "extract_fingerprint":"55".repeat(32), "lexical_fingerprint":"55".repeat(32),
            "sections":[
                {"ordinal":0,"heading":"Notes","body":"Hello \"world\"\n雪\\end",
                 "line_start":1,"line_end":2,"section_content_hash":"0ef7cc6214138de19a230af7c4f26b6a83e032f55b5b01bb652aed9fe381ef5d"},
                {"ordinal":1,"heading":"","body":"second","line_start":3,"line_end":3,
                 "section_content_hash":"a94dd4d3c2c6d2548ca4e560d72727bab5d795500191f5b85579130dd3b14603"}
            ]
        })
    );
    assert_eq!(
        intent.media_type,
        "application/vnd.lakeon.canonical-sections+json;version=1"
    );
    let event = intent.event.as_ref().unwrap();
    assert_eq!(event.job_kind, "semantic_content");
    assert_eq!(event.fingerprint, "77".repeat(32));
    assert_eq!(hex::encode(Sha256::digest(&bytes)), intent.payload.checksum);
    let row = client
        .query_one(
            "SELECT status,object_version_id FROM index_export_intents",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "uploading");
    assert!(
        row.get::<_, Option<String>>(1).is_none(),
        "materialization cannot ACK before immutable publication"
    );
}

#[tokio::test]
async fn changed_semantic_target_retires_only_the_old_intent_before_claiming_the_new_one() {
    let (_owned, mut client) = postgres().await;
    fixture(&client).await;
    client.batch_execute("UPDATE file_semantic_state SET desired_fingerprint=decode(repeat('88',32),'hex');
        INSERT INTO index_export_intents(event_id,entry_id,source_state_hash,fingerprint,incarnation,
            source_config_version,job_kind,content_hash,lexical_profile_hash,embedding_profile_id,media_type)
        SELECT '00000000-0000-0000-0000-000000000013',entry_id,source_state_hash,decode(repeat('88',32),'hex'),
            incarnation,source_config_version,job_kind,content_hash,lexical_profile_hash,embedding_profile_id,media_type
        FROM index_export_intents;").await.unwrap();
    let intent = claim_journal(&mut client, "folder_v2", &config())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(intent.event.as_ref().unwrap().fingerprint, "88".repeat(32));
    let row=client.query_one("SELECT status,object_version_id FROM index_export_intents WHERE event_id='00000000-0000-0000-0000-000000000012'",&[]).await.unwrap();
    assert_eq!(
        row.get::<_, String>(0),
        "dead",
        "superseded semantic work must not block clean handoff forever"
    );
    assert!(
        row.get::<_, Option<String>>(1).is_none(),
        "retirement is not an export ACK"
    );
}

#[tokio::test]
async fn lexical_work_in_progress_leaves_semantic_obligation_pending_without_a_lease() {
    let (_owned, mut client) = postgres().await;
    fixture(&client).await;
    client
        .execute(
            "UPDATE file_index_state SET desired_fingerprint=decode(repeat('99',32),'hex')",
            &[],
        )
        .await
        .unwrap();
    assert!(
        claim_journal(&mut client, "folder_v2", &config())
            .await
            .unwrap()
            .is_none()
    );
    let row = client
        .query_one(
            "SELECT status,attempts,lease_token::text FROM index_export_intents",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "pending");
    assert_eq!(row.get::<_, i32>(1), 0);
    assert!(row.get::<_, Option<String>>(2).is_none());
}

#[tokio::test]
async fn canonical_pg_to_versioned_object_readback_and_full_identity_ack() {
    let (_pg, mut client) = postgres().await;
    fixture(&client).await;
    native_fixture(&client).await;
    // Semantic work must not depend on retaining or re-reading raw source bytes.
    client
        .execute("DELETE FROM file_contents", &[])
        .await
        .unwrap();
    let (_obs, s3, mut config) = super::tests::minio().await;
    config.journal = true;
    let intent = claim_journal(&mut client, "folder_v2", &config)
        .await
        .unwrap()
        .unwrap();
    let first = publish(&s3, &config, &intent).await.unwrap();
    let replay = publish(&s3, &config, &intent).await.unwrap();
    assert_eq!(
        first, replay,
        "lost ACK must reuse the precise canonical object version"
    );
    let object = s3
        .get_object()
        .bucket(&config.bucket)
        .key(&first.0)
        .version_id(&first.1)
        .send()
        .await
        .unwrap();
    let metadata = object.metadata().unwrap();
    assert_eq!(
        metadata.get("x-index-input-kind").unwrap(),
        "canonical_sections"
    );
    assert_eq!(
        metadata.get("x-index-media-type").unwrap(),
        "application/vnd.lakeon.canonical-sections+json;version=1"
    );
    assert_eq!(
        metadata.get("x-index-lexical-profile-hash").unwrap(),
        &"33".repeat(32)
    );
    let bytes = object.body.collect().await.unwrap().into_bytes();
    let document: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(document["sections"][0]["body"], "Hello \"world\"\n雪\\end");
    acknowledge(&mut client, &intent, &first.0, &first.1, &first.2)
        .await
        .unwrap();
    let row = client
        .query_one(
            "SELECT status,object_version_id,object_size FROM index_export_intents",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "exported");
    assert_eq!(row.get::<_, String>(1), first.1);
    assert_eq!(row.get::<_, i64>(2), bytes.len() as i64);
    if let Ok(path) = std::env::var("LBFS_EXPORT_CANONICAL_PROOF_PATH") {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(path)
            .unwrap();
        file.write_all(&bytes).unwrap();
    }
}

#[tokio::test]
async fn corrupt_or_incomplete_canonical_sections_back_off_without_a_receipt() {
    for broken in [
        "UPDATE file_index_state SET active_fingerprint=decode(repeat('aa',31),'hex'),desired_fingerprint=decode(repeat('aa',31),'hex')",
        "UPDATE section_contents SET body='tampered' WHERE heading='Notes'",
        "DELETE FROM section_contents WHERE heading='Notes'",
        "UPDATE section_occurrences SET ordinal=4 WHERE ordinal=1",
        "DELETE FROM section_occurrences",
        "UPDATE section_occurrences SET line_end=0 WHERE ordinal=0",
    ] {
        let (_owned, mut client) = postgres().await;
        fixture(&client).await;
        client.batch_execute(broken).await.unwrap();
        assert!(
            claim_journal(&mut client, "folder_v2", &config())
                .await
                .is_err(),
            "{broken}"
        );
        let row=client.query_one("SELECT status,attempts,lease_token::text,object_version_id,available_at>clock_timestamp() FROM index_export_intents",&[]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "pending");
        assert_eq!(row.get::<_, i32>(1), 1);
        assert!(row.get::<_, Option<String>>(2).is_none());
        assert!(row.get::<_, Option<String>>(3).is_none());
        assert!(
            row.get::<_, bool>(4),
            "bad input must not monopolize the ready queue"
        );
    }
}

#[tokio::test]
async fn chunked_unicode_sections_roundtrip_and_encoded_size_is_bounded() {
    let (_owned, mut client) = postgres().await;
    fixture(&client).await;
    // Put escape sequences and multi-byte characters on both sides of the
    // character-chunk boundary; hash the unescaped source, not its JSON encoding.
    client.batch_execute("DELETE FROM section_occurrences WHERE ordinal=1;
        DELETE FROM section_contents;
        INSERT INTO section_contents SELECT sha256(convert_to('H'||chr(0+1)||'placeholder','UTF8')),decode(repeat('33',32),'hex'),'H',repeat('雪',262143)||chr(34)||chr(92)||chr(10)||repeat('a',262145);").await.unwrap();
    // PostgreSQL text forbids NUL, so build the content hash in BYTEA.
    client.execute("UPDATE section_contents SET section_content_hash=sha256(convert_to(heading,'UTF8')||decode('00','hex')||convert_to(body,'UTF8'))",&[]).await.unwrap();
    client.execute("UPDATE section_occurrences SET section_content_hash=(SELECT section_content_hash FROM section_contents)",&[]).await.unwrap();
    let intent = claim_journal(&mut client, "folder_v2", &config())
        .await
        .unwrap()
        .unwrap();
    let bytes = intent
        .payload
        .body()
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_bytes();
    let document: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let expected = format!("{}\"\\\n{}", "雪".repeat(262143), "a".repeat(262145));
    let body = document["sections"][0]["body"].as_str().unwrap();
    assert_eq!(body.len(), expected.len());
    assert!(
        body == expected,
        "chunked UTF-8/escaped body must round-trip exactly"
    );
    assert!(bytes.len() > SOURCE_CHUNK_BYTES);

    client.execute("UPDATE index_export_intents SET status='pending',lease_token=NULL,lease_expires_at=NULL",&[]).await.unwrap();
    client
        .execute(
            "UPDATE section_contents SET body=repeat(chr(1),12000000)",
            &[],
        )
        .await
        .unwrap();
    client.execute("UPDATE section_contents SET section_content_hash=sha256(convert_to(heading,'UTF8')||decode('00','hex')||convert_to(body,'UTF8'))",&[]).await.unwrap();
    client.execute("UPDATE section_occurrences SET section_content_hash=(SELECT section_content_hash FROM section_contents)",&[]).await.unwrap();
    let error = claim_journal(&mut client, "folder_v2", &config())
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("64MiB encoded limit"),
        "{error:#}"
    );
    let row = client
        .query_one(
            "SELECT status,object_version_id FROM index_export_intents",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "pending");
    assert!(row.get::<_, Option<String>>(1).is_none());
}

#[tokio::test]
async fn canonical_pagination_preserves_repeated_occurrences_in_order() {
    let (_owned, mut client) = postgres().await;
    fixture(&client).await;
    client.batch_execute("DELETE FROM section_occurrences WHERE ordinal=1;
        INSERT INTO section_occurrences SELECT o.entry_id,n,o.source_state_hash,o.section_content_hash,o.line_start,o.line_end
        FROM section_occurrences o CROSS JOIN generate_series(1,129) n;").await.unwrap();
    let intent = claim_journal(&mut client, "folder_v2", &config())
        .await
        .unwrap()
        .unwrap();
    let bytes = intent
        .payload
        .body()
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_bytes();
    let document: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let sections = document["sections"].as_array().unwrap();
    assert_eq!(sections.len(), 130);
    assert_eq!(sections[129]["ordinal"], 129);
    assert_eq!(sections[129]["body"], sections[0]["body"]);
    assert_eq!(
        sections[129]["section_content_hash"],
        "0ef7cc6214138de19a230af7c4f26b6a83e032f55b5b01bb652aed9fe381ef5d"
    );
}

#[tokio::test]
async fn canonical_text_uses_the_same_snapshot_as_its_section_identities() {
    let (owned, mut client) = postgres().await;
    fixture(&client).await;
    let transaction = materialization_transaction(&mut client).await.unwrap();
    transaction
        .query_one("SELECT count(*) FROM section_occurrences", &[])
        .await
        .unwrap();
    let update=std::process::Command::new("docker").args(["exec",&owned.0,"psql","-U","postgres","-c",
        "UPDATE folder_v2.section_contents SET body='concurrent replacement' WHERE heading='Notes'"]).output().unwrap();
    assert!(update.status.success());
    let payload = canonical::spool(
        &transaction,
        "folder_v2",
        "00000000-0000-0000-0000-000000000001",
        &"11".repeat(32),
        &"55".repeat(32),
        &"33".repeat(32),
    )
    .await
    .unwrap();
    let bytes = payload
        .body()
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_bytes();
    let document: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(document["sections"][0]["body"], "Hello \"world\"\n雪\\end");
    transaction.commit().await.unwrap();
    assert!(
        claim_journal(&mut client, "folder_v2", &config())
            .await
            .is_err(),
        "a fresh snapshot must detect the now-corrupt section"
    );
}

#[tokio::test]
async fn supersession_does_not_retire_other_incarnations_or_source_configurations() {
    let (_owned, mut client) = postgres().await;
    fixture(&client).await;
    client.batch_execute("UPDATE file_semantic_state SET desired_fingerprint=decode(repeat('88',32),'hex');
        INSERT INTO index_export_intents(event_id,entry_id,source_state_hash,fingerprint,incarnation,source_config_version,
            job_kind,content_hash,lexical_profile_hash,embedding_profile_id,media_type)
        SELECT v.id::uuid,i.entry_id,i.source_state_hash,i.fingerprint,v.inc,v.config,i.job_kind,i.content_hash,i.lexical_profile_hash,i.embedding_profile_id,i.media_type
        FROM index_export_intents i CROSS JOIN (VALUES
            ('00000000-0000-0000-0000-000000000013',2,1),('00000000-0000-0000-0000-000000000014',1,2)) v(id,inc,config);").await.unwrap();
    assert!(
        claim_journal(&mut client, "folder_v2", &config())
            .await
            .unwrap()
            .is_none()
    );
    let rows = client
        .query(
            "SELECT status FROM index_export_intents ORDER BY event_id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| r.get::<_, String>(0))
            .collect::<Vec<_>>(),
        vec!["dead", "pending", "pending"]
    );
}
