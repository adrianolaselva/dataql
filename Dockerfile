# DataQL — self-contained image. The DuckDB engine and all source drivers are
# embedded in the binary; only glibc + libstdc++ (present in debian-slim) are
# needed at runtime. Built on glibc (not Alpine/musl) so go-duckdb's prebuilt
# static library links.

# ---- builder ----
FROM golang:1.26-bookworm AS builder

ARG VERSION=dev
ARG COMMIT=docker
ARG BUILD_DATE=unknown

WORKDIR /src

# Cache modules first.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO build with the DuckDB engine embedded (no noduckdb stub). Link the C++/gcc
# runtimes statically; glibc/libstdc++ resolve at runtime.
RUN CGO_ENABLED=1 go build \
    -ldflags="-s -w -linkmode external -extldflags '-static-libgcc -static-libstdc++' \
      -X github.com/adrianolaselva/dataql/cmd.Version=${VERSION} \
      -X github.com/adrianolaselva/dataql/cmd.Commit=${COMMIT} \
      -X github.com/adrianolaselva/dataql/cmd.BuildDate=${BUILD_DATE}" \
    -o /out/dataql ./main.go

# ---- runtime ----
FROM debian:bookworm-slim

# ca-certificates lets remote sources (https URLs, S3, ...) verify TLS. This is
# OS metadata baked into the image, not a runtime download.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/dataql /usr/local/bin/dataql

# Default working directory for mounted data: `docker run -v "$PWD":/data ...`.
WORKDIR /data

ENTRYPOINT ["dataql"]
CMD ["--help"]
