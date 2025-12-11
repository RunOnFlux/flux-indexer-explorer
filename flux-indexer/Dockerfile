# FluxIndexer Dockerfile
# Multi-stage build with Flux daemon built from source

# Stage 1: Build Flux daemon from source
FROM ubuntu:22.04 AS flux-daemon

ENV DEBIAN_FRONTEND=noninteractive
ENV FLUX_VERSION=v9.0.5

WORKDIR /tmp

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libc6-dev \
    m4 \
    g++-multilib \
    autoconf \
    libtool \
    ncurses-dev \
    unzip \
    git \
    zlib1g-dev \
    wget \
    curl \
    bsdmainutils \
    automake \
    && rm -rf /var/lib/apt/lists/*

# Clone and build fluxd
RUN git clone --branch ${FLUX_VERSION} --depth 1 https://github.com/RunOnFlux/fluxd.git && \
    cd fluxd && \
    ./zcutil/fetch-params.sh && \
    ./zcutil/build.sh -j$(nproc) && \
    mkdir -p /flux-bin && \
    cp src/fluxd src/flux-cli src/flux-tx /flux-bin/ && \
    cp zcutil/fetch-params.sh /flux-bin/ && \
    chmod +x /flux-bin/* && \
    mkdir -p /zcash-params && \
    cp -r ~/.zcash-params/* /zcash-params/

# Stage 2: Build TypeScript (Indexer)
FROM node:20-alpine AS indexer-builder

WORKDIR /app

# Copy package files
COPY package*.json ./
COPY tsconfig.json ./

# Install dependencies
RUN npm ci

# Copy source code
COPY src/ ./src/

# Build TypeScript
RUN npm run build

# Stage 3: Build Frontend (Dashboard)
FROM node:20-alpine AS frontend-builder

WORKDIR /app/frontend

# Copy frontend package files
COPY frontend/package*.json ./

# Install frontend dependencies
RUN npm ci

# Copy frontend source
COPY frontend/ ./

# Build Next.js static export
RUN npm run build

# Stage 4: Production
FROM node:20-slim

WORKDIR /app

# Install runtime dependencies for Flux daemon and bootstrap import
RUN apt-get update && apt-get install -y --no-install-recommends \
    libstdc++6 \
    libgomp1 \
    libboost-system1.74.0 \
    libboost-filesystem1.74.0 \
    libboost-thread1.74.0 \
    libboost-program-options1.74.0 \
    libevent-2.1-7 \
    libzmq5 \
    supervisor \
    gettext-base \
    wget \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy Flux daemon binaries and zcash params from build stage
COPY --from=flux-daemon /flux-bin/* /usr/local/bin/
COPY --from=flux-daemon /zcash-params /root/.zcash-params

# Install Node.js production dependencies
COPY package*.json ./
RUN npm ci --only=production

# Copy built indexer from builder
COPY --from=indexer-builder /app/dist ./dist
COPY --from=indexer-builder /app/src/database/schema.sql ./dist/database/

# Copy built frontend from frontend-builder (static export)
COPY --from=frontend-builder /app/frontend/out ./frontend

# Create flux user and directories
RUN groupadd --system flux && \
    useradd --system --gid flux --home-dir /home/flux --create-home flux && \
    mkdir -p /home/flux/.flux && \
    chown -R flux:flux /home/flux

# Create fluxindexer user
RUN groupadd --system fluxindexer && \
    useradd --system --gid fluxindexer --home-dir /app fluxindexer && \
    chown -R fluxindexer:fluxindexer /app

# Copy docker configs
COPY docker/supervisord.conf /etc/supervisord.conf
COPY docker/flux.conf.template /home/flux/.flux/flux.conf.template
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh

# Create log directory and set permissions
RUN mkdir -p /var/log/supervisor && \
    chmod +x /usr/local/bin/entrypoint.sh

# Expose ports
EXPOSE 42067 16124

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=60s --retries=3 \
  CMD node -e "require('http').get('http://127.0.0.1:42067/health', (r) => {process.exit(r.statusCode === 200 ? 0 : 1)})"

# Start via entrypoint
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
