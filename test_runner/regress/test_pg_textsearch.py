from __future__ import annotations

import pytest

from fixtures.neon_fixtures import NeonEnv
from fixtures.pg_version import PgVersion


def test_pg_textsearch_bm25_index_and_query(neon_simple_env: NeonEnv):
    env = neon_simple_env
    if env.pg_version < PgVersion.V17:
        pytest.skip("pg_textsearch supports PostgreSQL 17+")

    endpoint = env.endpoints.create_start(
        "main",
        config_lines=["shared_preload_libraries='neon,pg_textsearch'"],
    )

    endpoint.safe_psql(
        """
        CREATE EXTENSION IF NOT EXISTS pg_textsearch;
        CREATE EXTENSION IF NOT EXISTS vector;
        CREATE TABLE documents (
            id bigserial PRIMARY KEY,
            content text NOT NULL,
            embedding vector(3)
        );
        INSERT INTO documents (content, embedding) VALUES
            ('PostgreSQL separates compute from storage', '[1,0,0]'),
            ('BM25 ranks full text search results', '[0,1,0]'),
            ('Hybrid retrieval combines vector and keyword search', '[0,0,1]'),
            ('GenericXLog keeps index page changes recoverable', '[1,1,0]');
        CREATE INDEX documents_bm25_idx
            ON documents USING bm25(content)
            WITH (text_config='english');
        CREATE INDEX documents_embedding_idx
            ON documents USING hnsw (embedding vector_l2_ops);
        """
    )

    text_hit = endpoint.safe_psql_scalar(
        """
        SELECT content
        FROM documents
        ORDER BY content <@> 'full text search'
        LIMIT 1
        """
    )
    assert "BM25" in text_hit

    endpoint.safe_psql(
        "INSERT INTO documents (content, embedding) VALUES ('Lakebase supports BM25 search', '[0,1,1]')"
    )
    incremental_hit = endpoint.safe_psql_scalar(
        """
        SELECT content
        FROM documents
        ORDER BY content <@> 'Lakebase BM25'
        LIMIT 1
        """
    )
    assert "Lakebase" in incremental_hit

    hybrid_hits = endpoint.safe_psql_scalar(
        """
        WITH
        vector_hits AS (
            SELECT id, row_number() OVER (ORDER BY embedding <-> '[0,1,1]'::vector) AS vrank
            FROM documents
            ORDER BY embedding <-> '[0,1,1]'::vector
            LIMIT 10
        ),
        text_hits AS (
            SELECT id, row_number() OVER (ORDER BY content <@> 'BM25 search') AS trank
            FROM documents
            ORDER BY content <@> 'BM25 search'
            LIMIT 10
        )
        SELECT count(*)
        FROM documents d
        LEFT JOIN vector_hits v USING (id)
        LEFT JOIN text_hits t USING (id)
        WHERE v.id IS NOT NULL OR t.id IS NOT NULL
        """
    )
    assert int(hybrid_hits) >= 2
