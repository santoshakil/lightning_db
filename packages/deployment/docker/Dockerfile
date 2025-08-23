# Lightning DB Production Docker Image
# Multi-stage build for optimal security and size

# Build stage
FROM rust:1.83-bookworm as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libc6-dev \
    protobuf-compiler \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Create app user for build
RUN adduser --disabled-password --gecos '' --uid 1000 appuser

# Set working directory
WORKDIR /app

# Copy dependency files first for better caching
COPY Cargo.toml Cargo.lock ./
COPY lightning_db_ffi/Cargo.toml ./lightning_db_ffi/
COPY lightning_db_wasm/Cargo.toml ./lightning_db_wasm/

# Build dependencies (cached layer)
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN mkdir lightning_db_ffi/src && echo "" > lightning_db_ffi/src/lib.rs
RUN mkdir lightning_db_wasm/src && echo "" > lightning_db_wasm/src/lib.rs
RUN cargo build --release --bin lightning-admin-server
RUN rm -rf src lightning_db_ffi/src lightning_db_wasm/src

# Copy source code
COPY . .

# Build the application
RUN cargo build --release --bin lightning-admin-server

# Verify the binary exists
RUN ls -la target/release/lightning-admin-server

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create app user
RUN adduser --disabled-password --gecos '' --uid 1000 appuser

# Create necessary directories
RUN mkdir -p /app/data /app/config /app/logs \
    && chown -R appuser:appuser /app

# Copy binary from builder
COPY --from=builder /app/target/release/lightning-admin-server /usr/local/bin/lightning-admin-server
RUN chmod +x /usr/local/bin/lightning-admin-server

# Copy configuration files
COPY packages/deployment/configs/prod/lightning_db.toml /app/config/

# Switch to app user
USER appuser

# Set working directory
WORKDIR /app

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Expose ports
EXPOSE 8080 9090

# Set environment variables
ENV RUST_LOG=info
ENV LIGHTNING_DB_CONFIG=/app/config/lightning_db.toml
ENV LIGHTNING_DB_DATA_DIR=/app/data
ENV LIGHTNING_DB_LOG_DIR=/app/logs

# Run the application
CMD ["lightning-admin-server"]