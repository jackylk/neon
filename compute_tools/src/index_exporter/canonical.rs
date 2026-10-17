//! Stream a validated current lexical snapshot into one immutable canonical input.
use super::*;

const MAX_SECTIONS: i64 = 100_000;
const PAGE_SECTIONS: i64 = 128;
// PostgreSQL substring(text) counts characters, each at most four UTF-8 bytes.
const TEXT_CHUNK_CHARACTERS: i32 = (SOURCE_CHUNK_BYTES / 4) as i32;

struct Writer {
    file: tokio::fs::File,
    len: usize,
    hash: Sha256,
}

impl Writer {
    async fn write(&mut self, bytes: &[u8]) -> Result<()> {
        let len = self
            .len
            .checked_add(bytes.len())
            .context("canonical size overflow")?;
        if len > MAX_FILE_BYTES {
            bail!("canonical input exceeds 64MiB encoded limit");
        }
        self.file.write_all(bytes).await?;
        self.hash.update(bytes);
        self.len = len;
        Ok(())
    }

    async fn text(
        &mut self,
        transaction: &tokio_postgres::Transaction<'_>,
        schema: &str,
        section: &str,
        profile: &str,
        column: &str,
        content_hash: &mut Sha256,
        native_hash: &mut Sha256,
    ) -> Result<()> {
        assert!(matches!(column, "heading" | "body"));
        let value = if column == "heading" {
            "coalesce(heading,'')"
        } else {
            "body"
        };
        let sql = format!(
            "SELECT substring({value} FROM $3::int FOR $4::int) FROM {}.section_contents \
            WHERE section_content_hash=decode($1,'hex') AND lexical_profile_hash=decode($2,'hex')",
            quote(schema)
        );
        self.write(b"\"").await?;
        let mut offset: i32 = 1;
        loop {
            let row = transaction
                .query_one(&sql, &[&section, &profile, &offset, &TEXT_CHUNK_CHARACTERS])
                .await?;
            let chunk: String = row.try_get(0)?;
            if chunk.is_empty() {
                break;
            }
            content_hash.update(chunk.as_bytes());
            native_hash.update(chunk.as_bytes());
            let encoded = serde_json::to_vec(&chunk)?;
            self.write(&encoded[1..encoded.len() - 1]).await?;
            offset = offset
                .checked_add(chunk.chars().count() as i32)
                .context("canonical text offset overflow")?;
        }
        self.write(b"\"").await
    }
}

pub(super) async fn spool(
    transaction: &tokio_postgres::Transaction<'_>,
    schema: &str,
    entry: &str,
    state: &str,
    fingerprint: &str,
    profile: &str,
) -> Result<Spool> {
    for hash in [state, fingerprint, profile] {
        if hash.len() != 64
            || !hash
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            bail!("invalid canonical SHA-256 identity");
        }
    }
    let s = quote(schema);
    let count: i64 = transaction
        .query_one(
            &format!(
                "SELECT count(*) FROM {s}.section_occurrences \
        WHERE entry_id=$1::text::uuid AND source_state_hash=decode($2,'hex')"
            ),
            &[&entry, &state],
        )
        .await?
        .get(0);
    if !(1..=MAX_SECTIONS).contains(&count) {
        bail!("canonical section count outside 1..100000");
    }
    let mut out = Writer {
        file: tokio::fs::File::from_std(tempfile::tempfile()?),
        len: 0,
        hash: Sha256::new(),
    };
    out.write(format!(r#"{{"schema":"canonical-sections/v1","source_state_hash":"{state}","extract_fingerprint":"{fingerprint}","lexical_fingerprint":"{fingerprint}","sections":["#).as_bytes()).await?;
    let sql = format!(
        "SELECT ordinal,line_start,line_end,encode(section_content_hash,'hex') \
        FROM {s}.section_occurrences WHERE entry_id=$1::text::uuid AND source_state_hash=decode($2,'hex') \
        AND ordinal >= $3 ORDER BY ordinal LIMIT {PAGE_SECTIONS}"
    );
    let mut ordinal: i32 = 0;
    loop {
        let page = transaction.query(&sql, &[&entry, &state, &ordinal]).await?;
        if page.is_empty() {
            break;
        }
        for row in page {
            let position: i32 = row.try_get(0)?;
            let start: i32 = row.try_get(1)?;
            let end: i32 = row.try_get(2)?;
            let section: String = row.try_get(3)?;
            if position != ordinal || start < 1 || end < start {
                bail!("invalid canonical section order or line range");
            }
            if ordinal > 0 {
                out.write(b",").await?;
            }
            out.write(format!(r#"{{"ordinal":{ordinal},"line_start":{start},"line_end":{end},"section_content_hash":"{section}","heading":"#).as_bytes()).await?;
            // Existing producers have two content identity encodings. Validate
            // both against the exact same snapshot; preserve the stored identity.
            let lengths = transaction.query_one(
                &format!("SELECT octet_length(coalesce(heading,'')),octet_length(body) FROM {s}.section_contents WHERE section_content_hash=decode($1,'hex') AND lexical_profile_hash=decode($2,'hex')"),
                &[&section, &profile],
            ).await?;
            let heading_bytes: i32 = lengths.try_get(0)?;
            let body_bytes: i32 = lengths.try_get(1)?;
            let mut hash = Sha256::new();
            let mut native = Sha256::new();
            native.update(("lbfs:section:v1".len() as i32).to_be_bytes());
            native.update(b"lbfs:section:v1");
            native.update(heading_bytes.to_be_bytes());
            out.text(
                transaction,
                schema,
                &section,
                profile,
                "heading",
                &mut hash,
                &mut native,
            )
            .await?;
            hash.update([0]);
            out.write(b",\"body\":").await?;
            native.update(body_bytes.to_be_bytes());
            out.text(
                transaction,
                schema,
                &section,
                profile,
                "body",
                &mut hash,
                &mut native,
            )
            .await?;
            if hex::encode(hash.finalize()) != section && hex::encode(native.finalize()) != section
            {
                bail!("canonical section text checksum mismatch");
            }
            out.write(b"}").await?;
            ordinal += 1;
        }
    }
    if ordinal as i64 != count {
        bail!("canonical section set incomplete");
    }
    out.write(b"]}").await?;
    out.file.flush().await?;
    Ok(Spool {
        file: out.file,
        len: out.len,
        checksum: hex::encode(out.hash.finalize()),
    })
}
