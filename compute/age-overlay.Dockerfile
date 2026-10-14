# Add Apache AGE to the currently deployed, dbay-patched PG17 Compute image.
# The full Compute Dockerfile also includes AGE for future complete builds.
# Build context: Neon repository root, with vendor/postgres-v17 checked out.

ARG BASE_IMAGE=swr.cn-north-4.myhuaweicloud.com/flex/compute-node-v17@sha256:028f84e3e3945084987847bac730ce81d86680ff9d87f726e63804c47fe95779
FROM ${BASE_IMAGE} AS builder
USER root

RUN find /etc/apt -name '*.sources' -o -name '*.list' | \
    xargs -r sed -i 's|mirrors.aliyun.com|deb.debian.org|g' && \
    apt-get update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates bison flex libreadline-dev zlib1g-dev \
    libssl-dev libxml2-dev libxslt1-dev liblz4-dev libzstd-dev libicu-dev \
    libossp-uuid-dev wget pkg-config && \
    rm -rf /var/lib/apt/lists/*

COPY vendor/postgres-v17 /build/postgres
WORKDIR /build/postgres
RUN ./configure CFLAGS='-O2 -g3 -fsigned-char' --prefix=/usr/local \
      --with-openssl --with-uuid=ossp --with-icu \
      --with-libxml --with-libxslt --with-lz4 --with-zstd && \
    make -C src/include install && \
    make -C src/makefiles install && \
    make -C src/common install && \
    make -C src/port install && \
    cp -a /usr/local/include/postgresql/server /usr/local/include/server && \
    cp -a /usr/local/include/postgresql/internal /usr/local/include/internal

WORKDIR /build
RUN wget https://github.com/apache/age/archive/e1467f12e0b1d15dd35d3ab93f057a7112d425b8.tar.gz -O age.tar.gz && \
    echo "da2a8c70033e08620ba9f9927b495908cc935ac80529d8f4d30cefae719efaa7 age.tar.gz" | sha256sum --check && \
    mkdir age && tar xzf age.tar.gz --strip-components=1 -C age
WORKDIR /build/age
RUN make -j "$(nproc)" PG_CONFIG=/usr/local/bin/pg_config && \
    make PG_CONFIG=/usr/local/bin/pg_config install

FROM ${BASE_IMAGE}
USER root
COPY --from=builder /usr/local/lib/age.so /usr/local/lib/age.so
COPY --from=builder /usr/local/share/extension/age.control /usr/local/share/extension/age.control
COPY --from=builder /usr/local/share/extension/age--*.sql /usr/local/share/extension/
RUN test -s /usr/local/lib/age.so && \
    test -s /usr/local/share/extension/age--1.7.0.sql && \
    /usr/local/bin/pg_config --version | grep -q '^PostgreSQL 17\.'
USER postgres
