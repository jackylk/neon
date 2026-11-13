# Build the complete Neon extension against the accepted PG17.5 headers.
ARG RUNTIME_IMAGE=swr.cn-north-4.myhuaweicloud.com/flex/compute-node-v17@sha256:18c316104b5c5e94a1296730d3ead2c49ddbafbcf8864d1306d777792229bc90
ARG COMMUNICATOR_IMAGE=lakeon-communicator-build:r379
ARG PG_HEADERS_IMAGE=lakeon-audit-addon-build:r12-metadata
FROM ${RUNTIME_IMAGE} AS runtime
FROM ${COMMUNICATOR_IMAGE} AS communicator
FROM ${PG_HEADERS_IMAGE} AS builder
COPY --from=communicator /home/nonroot/target-bin/libcommunicator.a /tmp/rust/libcommunicator.a
COPY --from=runtime /usr/local/lib/libpq.so* /usr/local/lib/
COPY pgxn/neon /tmp/neon/
WORKDIR /tmp/neon
RUN make all -j2 PG_CONFIG=/usr/local/bin/pg_config NEON_CARGO_ARTIFACT_TARGET_DIR=/tmp/rust libpq_srcdir=/tmp/postgres-source/src/interfaces/libpq libpq=/usr/local/lib/libpq.so -o /tmp/rust/libcommunicator.a -o communicator/communicator_bindings.h && mkdir -p /target-bin && cp neon.so /target-bin/neon.so && nm /target-bin/neon.so | grep neon_public_query_limits_init
