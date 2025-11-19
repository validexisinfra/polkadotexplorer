#!/usr/bin/env bash

###############################################################################
# Polkadot Telemetry Collector Installer
# --------------------------------------
# This script automatically installs:
#  - Python3 + virtual environment
#  - substrate-telemetry-client
#  - Polkadot telemetry collector
#  - Cron job that runs every 5 minutes
#
# Designed for usage:
#   source <(curl -s https://raw.githubusercontent.com/validexisinfra/polkadotexplorer/blob/main/telemetrycollector.sh)
###############################################################################

set -e

echo "[1/5] Updating system packages..."
apt update -y
apt install -y python3 python3-venv python3-pip curl

echo "[2/5] Preparing directories and Python virtual environment..."
mkdir -p /root/polkadot_telemetry

if [ ! -d /root/telem ]; then
    python3 -m venv /root/telem
fi

source /root/telem/bin/activate

echo "[3/5] Installing Python dependencies..."
pip install --upgrade pip
# Required version for correct feed parsing
pip install "substrate-telemetry-client==0.1.1"

echo "[4/5] Deploying telemetry collector script..."

cat > /root/polkadot_telemetry/collect_polkadot_telemetry.py << 'EOF'
#!/usr/bin/env python3
# ---------------------------------------------------------------------
# Polkadot Telemetry Collector
# Collects node data from the public telemetry feed and exports
# two CSV files:
#   - polkadot_nodes_latest.csv
#   - polkadot_nodes_<timestamp>.csv
#
# No raw fields, only clean structured metrics.
# ---------------------------------------------------------------------

import csv
import os
import time
from datetime import datetime, timezone

from substrate_telemetry_client import TelemetryClient, ChainGenesis


def collect_nodes():
    """Connects to telemetry feed and returns list of nodes."""
    with TelemetryClient(chain=ChainGenesis.POLKADOT) as client:
        time.sleep(5)  # give feed some time
        return client.get_nodes()


def parse_system_info(system_info):
    """Extract OS, CPU, RAM, kernel, distro from SystemInfo object."""
    if system_info is None:
        return {k: None for k in [
            "os","cpu_arch","cpu_model","cpu_cores",
            "memory_bytes","memory_gb","linux_distro",
            "linux_kernel","is_virtual_machine"
        ]}

    mem = getattr(system_info, "memory", None)
    mem_gb = mem / (1024 ** 3) if isinstance(mem, (int, float)) else None

    return {
        "os": getattr(system_info, "target_os", None),
        "cpu_arch": getattr(system_info, "target_arch", None),
        "cpu_model": getattr(system_info, "cpu", None),
        "cpu_cores": getattr(system_info, "core_count", None),
        "memory_bytes": mem,
        "memory_gb": mem_gb,
        "linux_distro": getattr(system_info, "linux_distro", None),
        "linux_kernel": getattr(system_info, "linux_kernel", None),
        "is_virtual_machine": getattr(system_info, "is_virtual_machine", None),
    }


def _agg(values):
    """Returns (last, avg, max) from a numeric list."""
    if not isinstance(values, (list, tuple)) or not values:
        return None, None, None
    return values[-1], sum(values)/len(values), max(values)


def parse_hardware(hw):
    """Extract upload/download BW aggregated metrics."""
    if hw is None:
        return {
            "upload_bw_last": None, "upload_bw_avg": None, "upload_bw_max": None,
            "download_bw_last": None, "download_bw_avg": None, "download_bw_max": None,
        }
    u_last, u_avg, u_max = _agg(getattr(hw, "upload", None))
    d_last, d_avg, d_max = _agg(getattr(hw, "download", None))
    return {
        "upload_bw_last": u_last, "upload_bw_avg": u_avg, "upload_bw_max": u_max,
        "download_bw_last": d_last, "download_bw_avg": d_avg, "download_bw_max": d_max,
    }


def parse_io(io):
    """Extract state cache aggregated metrics."""
    if io is None:
        return {
            "state_cache_size_last": None,
            "state_cache_size_avg": None,
            "state_cache_size_max": None,
        }
    last, avg, maxv = _agg(getattr(io, "state_cache_size", None))
    return {
        "state_cache_size_last": last,
        "state_cache_size_avg": avg,
        "state_cache_size_max": maxv,
    }


def compute_uptime(startup_ms, now_dt):
    """Calculate uptime (seconds) from startup timestamp."""
    if startup_ms is None:
        return None
    try:
        startup_ms = float(startup_ms)
    except:
        return None
    diff = now_dt.timestamp() * 1000 - startup_ms
    return diff / 1000 if diff >= 0 else None


def node_to_row(n, now_dt):
    """Convert telemetry NodeInfo to flat CSV row."""
    sysinfo = parse_system_info(getattr(n, "system_info", None))
    hwinfo = parse_hardware(getattr(n, "hardware", None))
    ioinfo = parse_io(getattr(n, "io", None))

    block = getattr(n, "block", None)
    net = getattr(n, "network_info", None)
    loc = getattr(n, "location", None)

    uptime = compute_uptime(getattr(n, "startup_time", None), now_dt)
    uptime_hours = uptime / 3600 if uptime else None

    return {
        "collected_at": now_dt.isoformat(),
        "node_id": getattr(n, "id", None),
        "name": getattr(n, "name", None),
        "validator": getattr(n, "validator", None),
        "implementation": getattr(n, "implementation", None),
        "version": getattr(n, "version", None),
        "stale": getattr(n, "stale", None),
        "startup_time": getattr(n, "startup_time", None),
        "updated_at": getattr(n, "updated_at", None),
        "tx_count": getattr(n, "transaction_count", None),
        "uptime_seconds": uptime,
        "uptime_hours": uptime_hours,

        # block info
        "block_height": getattr(block, "height", None) if block else None,
        "block_hash": getattr(block, "hash", None) if block else None,
        "block_finalized_height": getattr(block, "finalized", None) if block else None,
        "block_finalized_hash": getattr(block, "finalized_hash", None) if block else None,
        "block_propagation_ms": getattr(block, "propagation_time", None) if block else None,

        # network
        "peer_count": getattr(net, "peer_count", None) if net else None,
        "peer_id": getattr(net, "peer_id", None) if net else None,
        "ip": getattr(net, "ip", None) if net else None,

        # location
        "latitude": getattr(loc, "latitude", None) if loc else None,
        "longitude": getattr(loc, "longitude", None) if loc else None,
        "city": getattr(loc, "city", None) if loc else None,

        # system info
        **sysinfo,

        # bandwidth
        **hwinfo,

        # state cache
        **ioinfo,
    }


def write_csv(rows, base_dir):
    """Write latest CSV and timestamped CSV."""
    os.makedirs(base_dir, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    latest = os.path.join(base_dir, "polkadot_nodes_latest.csv")
    archive = os.path.join(base_dir, f"polkadot_nodes_{ts}.csv")

    with open(latest, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    with open(archive, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    print(f"Saved: {latest}")
    print(f"Saved: {archive}")


def main():
    base = "/root/polkadot_telemetry"
    print("Connecting to Polkadot telemetry feed...")

    nodes = collect_nodes()
    print(f"Nodes received: {len(nodes)}")

    now = datetime.now(timezone.utc)
    rows = [node_to_row(n, now) for n in nodes]
    write_csv(rows, base)


if __name__ == "__main__":
    main()
EOF

chmod +x /root/polkadot_telemetry/collect_polkadot_telemetry.py

echo "[5/5] Creating cron job (runs every 5 minutes)..."
cat > /etc/cron.d/polkadot_telemetry << "EOF"
*/5 * * * * root . /root/telem/bin/activate && cd /root/polkadot_telemetry && /root/telem/bin/python3 collect_polkadot_telemetry.py >> /root/polkadot_telemetry/collector.log 2>&1
EOF

chmod 644 /etc/cron.d/polkadot_telemetry

echo "✔ Polkadot telemetry collector installed successfully."
echo "✔ CSV files will appear in: /root/polkadot_telemetry"
echo "✔ Cron runs every 5 minutes."
