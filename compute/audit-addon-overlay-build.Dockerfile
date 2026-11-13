# Fast, reproducible rebuild against the accepted PG17.5 runtime headers.
# Named contexts must be clean vendor/postgres-v17 (gitlink 1e01fcea2a6b)
# and the verified unmodified pgauditlogtofile v1.6.4 source archive.
ARG BASE_IMAGE=swr.cn-north-4.myhuaweicloud.com/flex/compute-node-v17@sha256:ddf73fbde55d93a05f004e35251e5f86ae22ebc04c2a6042eae20ca9cd732bac
ARG BUILD_TOOLS_IMAGE=lakeon-compute-ctl-build:fs-rename-20261106
FROM ${BASE_IMAGE} AS runtime
FROM ${BUILD_TOOLS_IMAGE} AS builder
USER root
COPY --from=runtime /usr/local/bin/pg_config /usr/local/bin/pg_config
COPY --from=runtime /usr/local/lib/pgxs /usr/local/lib/pgxs
COPY --from=postgres-source / /tmp/postgres-source/
WORKDIR /tmp/postgres-source
RUN pg_config --version | grep -qx 'PostgreSQL 17.5' && \
    ./configure --prefix=/usr/local CFLAGS='-O2 -g3 -fsigned-char' \
      --enable-debug --with-openssl --with-uuid=ossp --with-icu \
      --with-libxml --with-libxslt --with-lz4 --with-zstd && \
    make -C src/backend -j2 generated-headers && make -C src/include install && \
    cp -a /usr/local/include/postgresql/. /usr/local/include/
COPY --from=audit-source / /tmp/audit-source/
COPY compute/patches/pgauditlogtofile-metadata-only.patch /tmp/metadata.patch
WORKDIR /tmp/audit-source
RUN patch -p1 < /tmp/metadata.patch && make -j2 PG_CONFIG=/usr/local/bin/pg_config && \
    mkdir -p /target-bin && cp pgauditlogtofile.so /target-bin/pgauditlogtofile.so
