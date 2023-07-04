FROM --platform=${BUILDPLATFORM:-linux/amd64} golang:1.20-bullseye AS builder
# This builds bifrost-gateway

ARG TARGETPLATFORM TARGETOS TARGETARCH

ENV GOPATH      /go
ENV SRC_PATH    $GOPATH/src/github.com/ipfs/bifrost-gateway
ENV GO111MODULE on
ENV GOPROXY     https://proxy.golang.org

COPY go.* $SRC_PATH/
WORKDIR $SRC_PATH
RUN go mod download

COPY . $SRC_PATH
RUN git config --global --add safe.directory /go/src/github.com/ipfs/bifrost-gateway

RUN --mount=target=. \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o $GOPATH/bin/bifrost-gateway

#------------------------------------------------------
FROM alpine:3.18

# This runs bifrost-gateway

# Instal binaries for $TARGETARCH
RUN apk add --no-cache tini su-exec ca-certificates

ENV GOPATH                 /go
ENV SRC_PATH               $GOPATH/src/github.com/ipfs/bifrost-gateway
ENV BIFROST_GATEWAY_PATH   /data/bifrost-gateway
ENV KUBO_RPC_URL           https://node0.delegate.ipfs.io,https://node1.delegate.ipfs.io,https://node2.delegate.ipfs.io,https://node3.delegate.ipfs.io

COPY --from=builder $GOPATH/bin/bifrost-gateway /usr/local/bin/bifrost-gateway
COPY --from=builder $SRC_PATH/docker/entrypoint.sh /usr/local/bin/entrypoint.sh

RUN mkdir -p $BIFROST_GATEWAY_PATH && \
    adduser -D -h $BIFROST_GATEWAY_PATH -u 1000 -G users ipfs && \
    chown ipfs:users $BIFROST_GATEWAY_PATH
VOLUME $BIFROST_GATEWAY_PATH

ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/entrypoint.sh"]

CMD ["--gateway-port", "8081", "--metrics-port", "8041"]
