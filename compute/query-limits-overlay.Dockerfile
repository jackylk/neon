# Retain accepted PG17/AGE/R12 runtime and replace the complete Neon extension.
ARG NEON_EXTENSION_IMAGE=lakeon-neon-extension-build:r379
ARG BASE_IMAGE=swr.cn-north-4.myhuaweicloud.com/flex/compute-node-v17@sha256:18c316104b5c5e94a1296730d3ead2c49ddbafbcf8864d1306d777792229bc90
FROM ${NEON_EXTENSION_IMAGE} AS extension
FROM ${BASE_IMAGE}
ARG BUILD_TAG
LABEL org.opencontainers.image.revision="${BUILD_TAG}"
USER root
COPY --from=extension /target-bin/neon.so /usr/local/lib/neon.so
RUN ldd /usr/local/lib/neon.so | tee /tmp/neon-libs && ! grep -q 'not found' /tmp/neon-libs
USER postgres
