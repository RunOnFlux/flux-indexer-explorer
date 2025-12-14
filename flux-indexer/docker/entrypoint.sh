#!/bin/sh
set -e

echo "=============================================="
echo "FluxIndexer Container Starting"
echo "=============================================="

# Create log directory
mkdir -p /var/log/supervisor

#######################################
# SECTION 1: ClickHouse Bootstrap
#######################################

# ClickHouse connection settings
CH_HOST="${CH_HOST:-clickhouse}"
CH_PORT="${CH_PORT:-9000}"
CH_HTTP_PORT="${CH_HTTP_PORT:-8123}"
CH_DATABASE="${CH_DATABASE:-fluxindexer}"
CH_USER="${CH_USER:-fluxindexer}"
CH_PASSWORD="${CH_PASSWORD:-}"

# Build clickhouse-client connection string
ch_client() {
  clickhouse-client \
    --host="$CH_HOST" \
    --port="$CH_PORT" \
    --user="$CH_USER" \
    --password="$CH_PASSWORD" \
    --database="$CH_DATABASE" \
    "$@"
}

# Wait for ClickHouse to be ready (up to 30 minutes for initial startup)
wait_for_clickhouse() {
  echo ""
  echo "[ClickHouse] Waiting for ClickHouse to be ready..."

  max_attempts=180  # 30 minutes at 10 second intervals
  attempt=1

  while [ $attempt -le $max_attempts ]; do
    if ch_client --query="SELECT 1" >/dev/null 2>&1; then
      echo "[ClickHouse] Connected successfully!"
      return 0
    fi

    if [ $((attempt % 6)) -eq 0 ]; then
      elapsed=$((attempt * 10 / 60))
      echo "[ClickHouse] Still waiting... (${elapsed} minutes elapsed)"
    fi

    sleep 10
    attempt=$((attempt + 1))
  done

  echo "[ClickHouse] ERROR: Could not connect to ClickHouse after 30 minutes"
  echo "[ClickHouse] Check that ClickHouse container is running and healthy"
  exit 1
}

# Check if ClickHouse needs bootstrap (no block data)
# Note: Schema is created by the indexer application, not by ClickHouse init scripts
# Bootstrap restores a full backup including schema and data
clickhouse_needs_bootstrap() {
  # First check if the blocks table exists in our database
  table_exists=$(ch_client --query="SELECT count() FROM system.tables WHERE database='$CH_DATABASE' AND name='blocks'" 2>/dev/null || echo "0")

  if [ "$table_exists" = "0" ]; then
    echo "[ClickHouse] No blocks table found - bootstrap needed (fresh database)"
    return 0  # Needs bootstrap
  fi

  # Check if there's any block data
  block_count=$(ch_client --query="SELECT count() FROM $CH_DATABASE.blocks" 2>/dev/null || echo "0")

  if [ "$block_count" = "0" ]; then
    echo "[ClickHouse] No block data found - bootstrap needed"
    return 0  # Needs bootstrap
  else
    echo "[ClickHouse] Found $block_count blocks in database"
    return 1  # Already has data
  fi
}

# Download and restore ClickHouse bootstrap
bootstrap_clickhouse() {
  local bootstrap_url="$1"
  # Shared volume: mounted at /clickhouse-backup in indexer, /var/lib/clickhouse/backup in ClickHouse
  local backup_dir="/clickhouse-backup"
  local ch_backup_path="/var/lib/clickhouse/backup"  # Path as seen by ClickHouse server
  local backup_name="fluxindexer_bootstrap"

  echo ""
  echo "[ClickHouse Bootstrap] Starting database bootstrap..."
  echo "[ClickHouse Bootstrap] URL: $bootstrap_url"

  # Create backup directory if it doesn't exist
  mkdir -p "$backup_dir"

  # Determine file type and download
  case "$bootstrap_url" in
    *.tar.gz|*.tgz)
      echo "[ClickHouse Bootstrap] Detected tar.gz format"
      local archive_file="$backup_dir/bootstrap.tar.gz"

      echo "[ClickHouse Bootstrap] Downloading bootstrap archive..."
      if ! wget --timeout=0 --tries=3 --no-check-certificate -q --show-progress -O "$archive_file" "$bootstrap_url"; then
        echo "[ClickHouse Bootstrap] ERROR: Failed to download bootstrap"
        return 1
      fi

      echo "[ClickHouse Bootstrap] Extracting archive..."
      mkdir -p "$backup_dir/$backup_name"
      if ! tar -xzf "$archive_file" -C "$backup_dir/$backup_name" --strip-components=0; then
        echo "[ClickHouse Bootstrap] ERROR: Failed to extract archive"
        rm -f "$archive_file"
        return 1
      fi

      # Cleanup archive immediately to free space
      rm -f "$archive_file"
      echo "[ClickHouse Bootstrap] Archive extracted and cleaned up"
      ;;

    *.zip)
      echo "[ClickHouse Bootstrap] Detected zip format"
      local archive_file="$backup_dir/bootstrap.zip"

      echo "[ClickHouse Bootstrap] Downloading bootstrap archive..."
      if ! wget --timeout=0 --tries=3 --no-check-certificate -q --show-progress -O "$archive_file" "$bootstrap_url"; then
        echo "[ClickHouse Bootstrap] ERROR: Failed to download bootstrap"
        return 1
      fi

      echo "[ClickHouse Bootstrap] Extracting archive..."
      mkdir -p "$backup_dir/$backup_name"
      if ! unzip -q "$archive_file" -d "$backup_dir/$backup_name"; then
        echo "[ClickHouse Bootstrap] ERROR: Failed to extract archive"
        rm -f "$archive_file"
        return 1
      fi

      rm -f "$archive_file"
      echo "[ClickHouse Bootstrap] Archive extracted and cleaned up"
      ;;

    *)
      echo "[ClickHouse Bootstrap] WARNING: Unknown format. Supported: .tar.gz, .tgz, .zip"
      return 1
      ;;
  esac

  # Find the actual backup directory (handle nested directories)
  # ClickHouse backups have a .backup file at the root
  local actual_backup_dir="$backup_dir/$backup_name"
  if [ ! -f "$actual_backup_dir/.backup" ]; then
    # Check one level deeper
    for subdir in "$actual_backup_dir"/*; do
      if [ -d "$subdir" ] && [ -f "$subdir/.backup" ]; then
        actual_backup_dir="$subdir"
        break
      fi
    done
  fi

  if [ ! -f "$actual_backup_dir/.backup" ]; then
    echo "[ClickHouse Bootstrap] ERROR: Invalid backup format - missing .backup metadata"
    rm -rf "$backup_dir/$backup_name"
    return 1
  fi

  echo "[ClickHouse Bootstrap] Found valid backup at: $actual_backup_dir"

  # Compute the path relative to backup_dir to get the subpath for ClickHouse
  local relative_path="${actual_backup_dir#$backup_dir/}"
  local ch_restore_path="$ch_backup_path/$relative_path"

  echo "[ClickHouse Bootstrap] ClickHouse restore path: $ch_restore_path"

  # Restore the backup using RESTORE command
  echo "[ClickHouse Bootstrap] Restoring database..."

  # First, ensure the database exists
  ch_client --query="CREATE DATABASE IF NOT EXISTS $CH_DATABASE" 2>/dev/null || true

  # Run RESTORE command with allow_non_empty_tables for idempotency
  # The path must be as seen by ClickHouse server, not the indexer container
  local restore_query="RESTORE DATABASE $CH_DATABASE FROM File('$ch_restore_path') SETTINGS allow_non_empty_tables=true"

  if ch_client --query="$restore_query" 2>&1; then
    echo "[ClickHouse Bootstrap] Database restored successfully!"

    # Verify restoration
    local restored_blocks=$(ch_client --query="SELECT count() FROM blocks" 2>/dev/null || echo "0")
    echo "[ClickHouse Bootstrap] Verified: $restored_blocks blocks in database"

    # Cleanup backup files to free space
    echo "[ClickHouse Bootstrap] Cleaning up backup files..."
    rm -rf "$backup_dir/$backup_name"

    return 0
  else
    echo "[ClickHouse Bootstrap] ERROR: RESTORE command failed"
    rm -rf "$backup_dir/$backup_name"
    return 1
  fi
}

# Main ClickHouse bootstrap logic
if [ -n "$CH_BOOTSTRAP_URL" ]; then
  echo ""
  echo "=============================================="
  echo "ClickHouse Bootstrap Configuration Detected"
  echo "=============================================="

  # Wait for ClickHouse to be ready first
  wait_for_clickhouse

  # Check if bootstrap is needed
  if clickhouse_needs_bootstrap; then
    if [ "$FORCE_CH_BOOTSTRAP" = "true" ]; then
      echo "[ClickHouse] FORCE_CH_BOOTSTRAP=true - proceeding with bootstrap"
    fi

    if bootstrap_clickhouse "$CH_BOOTSTRAP_URL"; then
      echo "[ClickHouse Bootstrap] Bootstrap completed successfully!"
    else
      echo "[ClickHouse Bootstrap] Bootstrap failed - indexer will sync from scratch"
      echo "[ClickHouse Bootstrap] This will take longer but data will still be indexed"
    fi
  else
    if [ "$FORCE_CH_BOOTSTRAP" = "true" ]; then
      echo "[ClickHouse] FORCE_CH_BOOTSTRAP=true detected, but data exists"
      echo "[ClickHouse] Clear ClickHouse volume to force re-bootstrap"
    fi
    echo "[ClickHouse] Skipping bootstrap - database already has data"
  fi
else
  echo ""
  echo "[ClickHouse] No CH_BOOTSTRAP_URL set - will sync from scratch or use existing data"

  # Still wait for ClickHouse to verify connectivity
  wait_for_clickhouse
fi

#######################################
# SECTION 2: Flux Daemon Configuration
#######################################

echo ""
echo "=============================================="
echo "Configuring Flux Daemon"
echo "=============================================="

# Set defaults
FLUX_RPC_USER=${FLUX_RPC_USER:-fluxrpc}
FLUX_RPC_PASSWORD=${FLUX_RPC_PASSWORD:-fluxrpc2024}

# Write config file directly
cat > /home/flux/.flux/flux.conf << EOF
server=1
rpcuser=${FLUX_RPC_USER}
rpcpassword=${FLUX_RPC_PASSWORD}
rpcport=16124
rpcallowip=127.0.0.1
rpcallowip=172.0.0.0/8
rpcallowip=::1
addressindex=1
timestampindex=1
spentindex=1
txindex=0
dbcache=1024
maxmempool=512
listen=0
listenonion=0
printtoconsole=1
logips=1
logtimestamps=1
disablewallet=1
rpcwarmup=1
EOF

# Set proper permissions
chown -R flux:flux /home/flux/.flux

#######################################
# SECTION 3: Zcash Parameters
#######################################

echo ""
echo "[Zcash Params] Checking Zcash parameters..."

ZCASH_PARAMS_DIR="/home/flux/.zcash-params"
mkdir -p "$ZCASH_PARAMS_DIR"

# Copy pre-downloaded params from build stage if they exist
if [ -d "/root/.zcash-params" ] && [ "$(ls -A /root/.zcash-params 2>/dev/null)" ]; then
  echo "[Zcash Params] Copying pre-downloaded parameters from build stage..."
  cp -r /root/.zcash-params/* "$ZCASH_PARAMS_DIR/"
  chown -R flux:flux "$ZCASH_PARAMS_DIR"
fi

# Expected minimum file sizes (in bytes) to validate downloads
SAPLING_SPEND_MIN=47958396    # ~46MB
SAPLING_OUTPUT_MIN=3592860    # ~3.4MB
SPROUT_GROTH16_MIN=725523612  # ~692MB

# Function to check if file exists and has correct size
check_param_file() {
  local file="$1"
  local min_size="$2"

  if [ ! -f "$file" ]; then
    return 1
  fi

  local actual_size=$(stat -c%s "$file" 2>/dev/null || echo "0")
  if [ "$actual_size" -lt "$min_size" ]; then
    echo "  WARNING: $file is incomplete (${actual_size} bytes, expected at least ${min_size} bytes)"
    rm -f "$file"
    return 1
  fi

  return 0
}

# Check if all parameters are present and valid
need_download=0
check_param_file "$ZCASH_PARAMS_DIR/sapling-spend.params" "$SAPLING_SPEND_MIN" || need_download=1
check_param_file "$ZCASH_PARAMS_DIR/sapling-output.params" "$SAPLING_OUTPUT_MIN" || need_download=1
check_param_file "$ZCASH_PARAMS_DIR/sprout-groth16.params" "$SPROUT_GROTH16_MIN" || need_download=1

if [ "$need_download" -eq 1 ]; then
  echo "[Zcash Params] Downloading Zcash parameters (first time only, ~900MB)..."
  echo "[Zcash Params] This may take several minutes depending on your connection..."

  cd "$ZCASH_PARAMS_DIR"

  # Download with retry logic from Flux official mirror (files are split into .part.1 and .part.2)
  download_with_retry() {
    local filename="$1"
    local base_url="https://download.runonflux.io/downloads"

    echo "  - Downloading $filename from Flux mirror..."

    # Download part 1
    echo "    Downloading part 1..."
    if ! wget --timeout=120 --tries=5 --progress=dot:giga --continue --retry-connrefused --waitretry=3 \
         -O "${filename}.part.1" "${base_url}/${filename}.part.1"; then
      echo "    Failed to download part 1"
      return 1
    fi

    # Download part 2
    echo "    Downloading part 2..."
    if ! wget --timeout=120 --tries=5 --progress=dot:giga --continue --retry-connrefused --waitretry=3 \
         -O "${filename}.part.2" "${base_url}/${filename}.part.2"; then
      echo "    Failed to download part 2"
      return 1
    fi

    # Concatenate parts
    echo "    Combining parts..."
    cat "${filename}.part.1" "${filename}.part.2" > "${filename}"
    rm -f "${filename}.part.1" "${filename}.part.2"

    return 0
  }

  if ! check_param_file "sapling-spend.params" "$SAPLING_SPEND_MIN"; then
    download_with_retry "sapling-spend.params" || {
      echo "ERROR: Failed to download sapling-spend.params from all mirrors"
      echo "Please check your network connection and try again"
      exit 1
    }
  fi

  if ! check_param_file "sapling-output.params" "$SAPLING_OUTPUT_MIN"; then
    download_with_retry "sapling-output.params" || {
      echo "ERROR: Failed to download sapling-output.params from all mirrors"
      exit 1
    }
  fi

  if ! check_param_file "sprout-groth16.params" "$SPROUT_GROTH16_MIN"; then
    download_with_retry "sprout-groth16.params" || {
      echo "ERROR: Failed to download sprout-groth16.params from all mirrors"
      exit 1
    }
  fi

  chown -R flux:flux "$ZCASH_PARAMS_DIR"
  echo "[Zcash Params] Parameters downloaded and validated successfully!"
else
  echo "[Zcash Params] All parameters present and validated."
fi

#######################################
# SECTION 4: Flux Daemon Bootstrap
#######################################

FLUX_DATA_DIR="/home/flux/.flux"

if [ -n "$BOOTSTRAP_URL" ]; then
  echo ""
  echo "=============================================="
  echo "Flux Daemon Bootstrap Configuration Detected"
  echo "=============================================="
  echo "[Daemon Bootstrap] URL: $BOOTSTRAP_URL"

  # Check if blockchain data already exists
  if [ -d "$FLUX_DATA_DIR/blocks" ] && [ "$(ls -A $FLUX_DATA_DIR/blocks 2>/dev/null)" ]; then
    echo "[Daemon Bootstrap] Blockchain data already exists."

    if [ "$FORCE_BOOTSTRAP" = "true" ]; then
      echo "[Daemon Bootstrap] FORCE_BOOTSTRAP=true - clearing existing data..."
      rm -rf "$FLUX_DATA_DIR/blocks" "$FLUX_DATA_DIR/chainstate" "$FLUX_DATA_DIR/database"
    else
      echo "[Daemon Bootstrap] Skipping bootstrap - using existing data."
      echo "[Daemon Bootstrap] Set FORCE_BOOTSTRAP=true to force re-download."
    fi
  fi

  # Download and extract bootstrap if needed
  if [ ! -d "$FLUX_DATA_DIR/blocks" ] || [ -z "$(ls -A $FLUX_DATA_DIR/blocks 2>/dev/null)" ]; then
    echo "[Daemon Bootstrap] Downloading blockchain bootstrap..."
    echo "[Daemon Bootstrap] This may take a while depending on file size and network speed..."

    cd /tmp

    # Detect file extension for proper extraction
    case "$BOOTSTRAP_URL" in
      *.tar.gz|*.tgz)
        echo "[Daemon Bootstrap] Detected tar.gz format"
        if wget --timeout=0 --tries=3 --no-check-certificate -q --show-progress -O bootstrap.tar.gz "$BOOTSTRAP_URL"; then
          echo "[Daemon Bootstrap] Extracting blockchain bootstrap..."
          tar -xzf bootstrap.tar.gz -C "$FLUX_DATA_DIR"
          rm -f bootstrap.tar.gz
          chown -R flux:flux "$FLUX_DATA_DIR"
          echo "[Daemon Bootstrap] Bootstrap extracted successfully!"
        else
          echo "[Daemon Bootstrap] ERROR: Failed to download bootstrap"
          echo "[Daemon Bootstrap] Continuing with normal sync from genesis..."
        fi
        ;;
      *.zip)
        echo "[Daemon Bootstrap] Detected zip format"
        if wget --timeout=0 --tries=3 --no-check-certificate -q --show-progress -O bootstrap.zip "$BOOTSTRAP_URL"; then
          echo "[Daemon Bootstrap] Extracting blockchain bootstrap..."
          unzip -q bootstrap.zip -d "$FLUX_DATA_DIR"
          rm -f bootstrap.zip
          chown -R flux:flux "$FLUX_DATA_DIR"
          echo "[Daemon Bootstrap] Bootstrap extracted successfully!"
        else
          echo "[Daemon Bootstrap] ERROR: Failed to download bootstrap"
          echo "[Daemon Bootstrap] Continuing with normal sync from genesis..."
        fi
        ;;
      *)
        echo "[Daemon Bootstrap] WARNING: Unknown format. Supported: .tar.gz, .tgz, .zip"
        echo "[Daemon Bootstrap] Continuing with normal sync from genesis..."
        ;;
    esac
  fi
else
  echo ""
  echo "[Daemon Bootstrap] No BOOTSTRAP_URL set - will sync from genesis or use existing data"
fi

#######################################
# SECTION 5: Start Services
#######################################

echo ""
echo "=============================================="
echo "Configuration Summary"
echo "=============================================="
echo "Flux Daemon:"
echo "  RPC Port: 16124"
echo "  RPC User: ${FLUX_RPC_USER}"
echo "  Data Directory: /home/flux/.flux"
echo ""
echo "FluxIndexer:"
echo "  API Port: ${API_PORT:-42067}"
echo "  ClickHouse: ${CH_HOST}:${CH_HTTP_PORT}"
echo "  Database: ${CH_DATABASE}"
echo ""

echo "=============================================="
echo "Starting Services (fluxd + fluxindexer)"
echo "=============================================="
exec /usr/bin/supervisord -n -c /etc/supervisord.conf
