# syntax=docker/dockerfile:1
# One Dockerfile for every Go binary in the repo; choose it with
#   docker build --build-arg SERVICE=inventory .
ARG GO_VERSION=1.27

FROM golang:${GO_VERSION} AS build
WORKDIR /src
ENV CGO_ENABLED=0
# Dependencies first, so source edits don't invalidate the module cache layer.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG SERVICE
ARG VERSION=dev
ARG COMMIT=unknown
RUN test -n "${SERVICE}" || (echo "build with --build-arg SERVICE=<directory under cmd/>" >&2 && exit 1)
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath \
      -ldflags "-s -w \
        -X github.com/Sanjay-Mx21/holdfast/internal/platform/buildinfo.Version=${VERSION} \
        -X github.com/Sanjay-Mx21/holdfast/internal/platform/buildinfo.Commit=${COMMIT}" \
      -o /out/app ./cmd/${SERVICE}

# Distroless static: no shell, no package manager, runs as an unprivileged user.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
EXPOSE 8080 9090
ENTRYPOINT ["/app"]
