FROM --platform=$BUILDPLATFORM golang:1.20-bullseye as builder

ENV GO111MODULE=on CGO_ENABLED=0
WORKDIR /work
ARG TARGETOS TARGETARCH

RUN --mount=target=. \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/client ./cmd/client

RUN --mount=target=. \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/server ./cmd/server

FROM gcr.io/distroless/static-debian11:latest

COPY --from=builder /out/client /usr/bin/client
COPY --from=builder /out/server /usr/bin/server

ARG image_version
ENV RELEASE=$image_version

WORKDIR /root