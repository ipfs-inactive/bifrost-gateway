FROM --platform=${BUILDPLATFORM:-linux/amd64} golang:1.20-bullseye AS builder
MAINTAINER IPFS Stewards <w3dt-stewards-ip@protocol.ai>

# This dockerfile builds and runs bifrost-gateway

ARG TARGETPLATFORM
ARG BUILDPLATFORM
ARG TARGETOS
ARG TARGETARCH

ENV GOPATH      /go
ENV SRC_PATH    $GOPATH/src/github.com/ipfs/bifrost-gateway
ENV GO111MODULE on
ENV GOPROXY     https://proxy.golang.org

ENV SUEXEC_VERSION v0.2
ENV TINI_VERSION v0.19.0
RUN set -eux; \
    dpkgArch="$(dpkg --print-architecture)"; \
    case "${dpkgArch##*-}" in \
        "amd64" | "armhf" | "arm64") tiniArch="tini-static-$dpkgArch" ;;\
        *) echo >&2 "unsupported architecture: ${dpkgArch}"; exit 1 ;; \
    esac; \
  cd /tmp \
  && git clone https://github.com/ncopa/su-exec.git \
  && cd su-exec \
  && git checkout -q $SUEXEC_VERSION \
  && make su-exec-static \
  && cd /tmp \
  && wget -q -O tini https://github.com/krallin/tini/releases/download/$TINI_VERSION/$tiniArch \
  && chmod +x tini

# Get the TLS CA certificates, they're not provided by busybox.
RUN apt-get update && apt-get install -y ca-certificates

COPY --chown=1000:users go.* $SRC_PATH/
WORKDIR $SRC_PATH
RUN go mod download

COPY --chown=1000:users . $SRC_PATH
RUN git config --global --add safe.directory /go/src/github.com/ipfs/bifrost-gateway
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o $GOPATH/bin/bifrost-gateway


#------------------------------------------------------
FROM --platform=${BUILDPLATFORM:-linux/amd64} busybox:1.34.1-glibc
MAINTAINER IPFS Stewards <w3dt-stewards-ip@protocol.ai>

ENV GOPATH                 /go
ENV SRC_PATH               /go/src/github.com/ipfs/bifrost-gateway
ENV BIFROST_GATEWAY_PATH   /data/bifrost-gateway
ENV KUBO_RPC_URL           https://node0.delegate.ipfs.io,https://node1.delegate.ipfs.io,https://node2.delegate.ipfs.io,https://node3.delegate.ipfs.io

COPY --from=builder $GOPATH/bin/bifrost-gateway /usr/local/bin/bifrost-gateway
COPY --from=builder $SRC_PATH/docker/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY --from=builder /tmp/su-exec/su-exec-static /sbin/su-exec
COPY --from=builder /tmp/tini /sbin/tini
COPY --from=builder /etc/ssl/certs /etc/ssl/certs

RUN mkdir -p $BIFROST_GATEWAY_PATH && \
    adduser -D -h $BIFROST_GATEWAY_PATH -u 1000 -G users ipfs && \
    chown ipfs:users $BIFROST_GATEWAY_PATH

VOLUME $BIFROST_GATEWAY_PATH
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/entrypoint.sh"]

CMD ["--gateway-port", "8081", "--metrics-port", "8041"]
