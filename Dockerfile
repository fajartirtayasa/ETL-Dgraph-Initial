# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.25.3
ARG TARGETOS=linux
ARG TARGETARCH=amd64

FROM golang:${GO_VERSION}-bookworm AS builder

WORKDIR /src

COPY go.mod go.sum ./

RUN --mount=type=cache,target=/go/pkg/mod \
  go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
  --mount=type=cache,target=/root/.cache/go-build \
  CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
  go build -trimpath -ldflags="-s -w" -o /out/traceability-dgraph-etl ./main.go

FROM gcr.io/distroless/base-debian12:nonroot

WORKDIR /app

COPY --from=builder /out/traceability-dgraph-etl /usr/local/bin/traceability-dgraph-etl
COPY config/ ./config/
COPY schema.dql .

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/traceability-dgraph-etl"]

