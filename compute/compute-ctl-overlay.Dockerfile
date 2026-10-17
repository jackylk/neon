# Rebuild the compute-ctl target first, then replace only the compiled binary
# in the accepted dbay PG17/AGE runtime. Its startup wrapper invokes
# compute_ctl.orig and must remain intact.
#
# docker buildx build --platform linux/amd64 --target compute-ctl --load \
#   --build-arg PG_VERSION=v17 --build-arg BUILD_TAG=<source-sha> \
#   -f compute/compute-node.Dockerfile -t lakeon-compute-ctl-build:<source-sha> .
# docker buildx build --platform linux/amd64 --load \
#   --build-arg COMPUTE_CTL_IMAGE=lakeon-compute-ctl-build:<source-sha> \
#   --build-arg BUILD_TAG=<source-sha> \
#   -f compute/compute-ctl-overlay.Dockerfile -t <runtime-image:immutable-tag> .
ARG COMPUTE_CTL_IMAGE=lakeon-compute-ctl-build:latest
ARG BASE_IMAGE=swr.cn-north-4.myhuaweicloud.com/flex/compute-node-v17@sha256:779c1b34e9d12c422a486f01f3b67cb402e4ab9fd1888fcdbe13ae439eeb636c
FROM ${COMPUTE_CTL_IMAGE} AS compiler
FROM ${BASE_IMAGE}
ARG BUILD_TAG
LABEL org.opencontainers.image.revision="${BUILD_TAG}"
# The retained runtime installs C.utf8, not its inherited en_US.utf8 default.
# Empty pooled Computes run initdb before exposing HTTP; an unavailable LANG
# prevents that path from ever becoming Ready. Existing database collations
# remain recorded in their timelines.
ENV LANG=C.UTF-8
USER root
RUN test -n "${BUILD_TAG}" && \
    grep -q 'compute_ctl.orig' /usr/local/bin/compute_ctl && \
    test -x /usr/local/bin/compute_ctl.orig && \
    test -s /usr/local/lib/age.so && \
    /usr/local/bin/postgres --version | grep -q '^postgres (PostgreSQL) 17\.'
COPY --from=compiler --chown=root:root --chmod=0755 /home/nonroot/target-bin/compute_ctl /usr/local/bin/compute_ctl.orig
RUN /usr/local/bin/compute_ctl.orig --help >/dev/null
USER postgres
