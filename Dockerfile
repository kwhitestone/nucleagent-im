# syntax=docker/dockerfile:1
# =============================================================================
# nucleagent-im image.
#
# Build context must be the workspace root because go.mod replace directives
# point to prism-fusion, nucleagent-shared, nucleagent-storage-ndcs and
# nucleagent-storage/app/src/server:
#
#   docker build -t nucleagent-im -f nucleagent-im/Dockerfile .
# =============================================================================

FROM golang:1.26 AS go-build
ENV CGO_ENABLED=0 GO111MODULE=on GOPROXY=https://goproxy.cn,direct
WORKDIR /build

# ndcs is a build requirement, not an im-specific one: prism-fusion's own
# initialize/gorm.go, utils/rotatelogs.go and the auth addon import it, and the
# shared auth plugin is now in im's build graph. ndcs in turn imports
# nucleagent-storage/contracts/provider, so storage has to be staged too.
COPY nucleagent-im/app/src/server/      ./nucleagent-im/app/src/server/
COPY prism-fusion/src/server/           ./prism-fusion/src/server/
COPY nucleagent-shared/                 ./nucleagent-shared/
COPY nucleagent-storage-ndcs/           ./nucleagent-storage-ndcs/
COPY nucleagent-storage/app/src/server/ ./nucleagent-storage/app/src/server/

WORKDIR /build/nucleagent-im/app/src/server
RUN go build -ldflags="-s -w" -o /out/nucleagent-im .

FROM alpine:3.20 AS final

RUN apk add --no-cache ca-certificates tzdata wget && \
    adduser -D -u 10001 -h /opt im

RUN mkdir -p /opt/log && chown -R im:im /opt

COPY --from=go-build /out/nucleagent-im /usr/local/bin/nucleagent-im
COPY nucleagent-im/app/src/server/config.yaml /opt/config.yaml

WORKDIR /opt
USER im

EXPOSE 26655

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD wget -qO- "http://127.0.0.1:${GATEWAY_PORT:-26655}/api/v1/im/health" || exit 1

ENTRYPOINT ["/usr/local/bin/nucleagent-im"]
CMD []
