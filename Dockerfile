# Build stage
FROM rust:1.75-alpine AS builder

# Install build dependencies
RUN apk add --no-cache musl-dev protoc

WORKDIR /build

# Copy manifests
COPY Cargo.toml Cargo.lock ./
COPY lightning_db_ffi/Cargo.toml ./lightning_db_ffi/

# Build dependencies (this is cached if manifests don't change)
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    mkdir lightning_db_ffi/src && \
    echo "fn main() {}" > lightning_db_ffi/src/main.rs && \
    cargo build --release && \
    rm -rf src lightning_db_ffi/src

# Copy source code
COPY . .

# Build the project
RUN cargo build --release --bin lightning-cli

# Runtime stage
FROM alpine:3.19

# Install runtime dependencies
RUN apk add --no-cache libgcc

# Create non-root user
RUN addgroup -g 1000 lightning && \
    adduser -D -u 1000 -G lightning lightning

# Copy binary from builder
COPY --from=builder /build/target/release/lightning-cli /usr/local/bin/

# Create data directory
RUN mkdir -p /data && chown lightning:lightning /data

# Switch to non-root user
USER lightning

# Set working directory
WORKDIR /data

# Expose default port (if your CLI has a server mode)
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD lightning-cli --version || exit 1

# Default command
ENTRYPOINT ["lightning-cli"]
CMD ["--help"]