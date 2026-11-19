#!/usr/bin/env bash
# Build two synthetic OpenShift-style Prometheus v2.55.1 TSDB dump archives (.tar.gz)
# for trying prometheus-tsdb-reader end to end:
#   1. openshift-tsdb-clean.tar.gz: compacted 2h ULID blocks + live WAL + chunks_head + lock + metadata
#   2. openshift-tsdb-damaged.tar.gz: same dump with 1 truncated block index + 1 torn WAL tail record
#
# Populates every metric and recording rule used by named/queries.json:
#   pod-cpu, pod-memory, node-cpu, node-iowait, node-load, node-memory,
#   node-network, node-disk-write-latency, node-rules, and vm-filesystem.
#
# Safe to re-run: overwrites the two archives in output_dir and removes its temporary work directory on exit.
#
# Usage:
#   ./examples/make-dump.sh [output_dir]

set -euo pipefail

OUT_DIR="${1:-$(pwd)/tsdb-fixtures}"
PROM_IMAGE="${PROM_IMAGE:-quay.io/prometheus/prometheus:v2.55.1@sha256:2659f4c2ebb718e7695cb9b25ffa7d6be64db013daba13e05c875451cf51b0d3}"
PY_IMAGE="${PY_IMAGE:-python:3.12-alpine@sha256:c5ba3deef00c7758561a0f03c26f44f10589263d40e732b8748e0d1f38f6dd89}"

if command -v docker >/dev/null 2>&1; then
  RUNTIME="docker"
elif command -v podman >/dev/null 2>&1; then
  RUNTIME="podman"
else
  echo "error: neither docker nor podman found in PATH" >&2
  exit 1
fi

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/prom-fixture.XXXXXX")"
EXPORTER_CID=""
PROM_CID=""

cleanup() {
  if [[ -n "${PROM_CID}" ]]; then
    "${RUNTIME}" rm -f "${PROM_CID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${EXPORTER_CID}" ]]; then
    "${RUNTIME}" rm -f "${EXPORTER_CID}" >/dev/null 2>&1 || true
  fi
  rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

mkdir -p "${OUT_DIR}" "${WORK_DIR}/prometheus"
chmod 0777 "${WORK_DIR}" "${WORK_DIR}/prometheus"

echo "==> [1/5] Generating 4h of historical OpenShift incident OpenMetrics (30s step)..."
python3 - "${WORK_DIR}" <<'PYEOF'
import math
import pathlib
import sys
import time

work = pathlib.Path(sys.argv[1])
now = int(time.time())
# Backfill [now - 4h, now - 60s] so compacted blocks end right before the live scrape window.
end_ts = (now - 60) // 30 * 30
start_ts = end_ts - 4 * 3600
step = 30
timestamps = list(range(start_ts, end_ts + 1, step))

# Simulated incident window: [end_ts - 90m, end_ts - 35m] on worker-1.example.com:9100
inc_start = end_ts - 90 * 60
inc_end = end_ts - 35 * 60


def incident_factor(ts: int) -> float:
    if ts < inc_start or ts > inc_end:
        return 0.0
    mid = (inc_start + inc_end) / 2.0
    half = (inc_end - inc_start) / 2.0
    return max(0.0, math.cos(((ts - mid) / half) * (math.pi / 2.0)))


families: list[tuple[str, str, str, list[str]]] = []


def add_family(name: str, mtype: str, help_text: str, lines: list[str]) -> None:
    families.append((name, mtype, help_text, lines))


nodes = ["worker-0.example.com:9100", "worker-1.example.com:9100"]
incident_node = "worker-1.example.com:9100"

# 1. node_cpu_seconds_total (4 cores per node)
cpu_lines: list[str] = []
last_cpu: dict[tuple[str, str, str], float] = {}
for node in nodes:
    for cpu in ("0", "1", "2", "3"):
        cum = {"idle": 10000.0, "user": 1200.0, "system": 400.0, "iowait": 50.0}
        for ts in timestamps:
            inc = incident_factor(ts) if node == incident_node else 0.0
            iow = 0.02 + (0.42 * inc)
            usr = 0.18 + (0.25 * inc)
            sys_v = 0.06 + (0.08 * inc)
            idl = max(0.02, 1.0 - (iow + usr + sys_v))
            cum["idle"] += idl * step
            cum["user"] += usr * step
            cum["system"] += sys_v * step
            cum["iowait"] += iow * step
            for mode, val in cum.items():
                cpu_lines.append(
                    f'node_cpu_seconds_total{{cpu="{cpu}",instance="{node}",job="node-exporter",mode="{mode}"}} {val:.3f} {ts}'
                )
        for mode, val in cum.items():
            last_cpu[(node, cpu, mode)] = val
add_family("node_cpu_seconds_total", "counter", "Seconds the CPUs spent in each mode.", cpu_lines)

# 2. node_load1 / node_load5 / node_load15 + recording rule instance:node_load1_per_cpu:ratio
for metric, scale in (("node_load1", 1.0), ("node_load5", 0.85), ("node_load15", 0.65)):
    lines = []
    for node in nodes:
        for ts in timestamps:
            inc = incident_factor(ts) if node == incident_node else 0.0
            val = 0.9 + (5.8 * inc * scale)
            lines.append(f'{metric}{{instance="{node}",job="node-exporter"}} {val:.3f} {ts}')
    add_family(metric, "gauge", f"{metric} load average.", lines)

load_ratio_lines = []
for node in nodes:
    for ts in timestamps:
        inc = incident_factor(ts) if node == incident_node else 0.0
        val = (0.9 + 5.8 * inc) / 4.0
        load_ratio_lines.append(f'instance:node_load1_per_cpu:ratio{{instance="{node}"}} {val:.4f} {ts}')
add_family("instance:node_load1_per_cpu:ratio", "gauge", "Load1 normalized by CPU count.", load_ratio_lines)

# 3. Node memory
mem_total = 32 * 1024**3
mem_tot_lines, mem_avail_lines = [], []
for node in nodes:
    for ts in timestamps:
        inc = incident_factor(ts) if node == incident_node else 0.0
        used_ratio = 0.48 + (0.41 * inc)
        avail = int(mem_total * (1.0 - used_ratio))
        mem_tot_lines.append(f'node_memory_MemTotal_bytes{{instance="{node}",job="node-exporter"}} {mem_total} {ts}')
        mem_avail_lines.append(f'node_memory_MemAvailable_bytes{{instance="{node}",job="node-exporter"}} {avail} {ts}')
add_family("node_memory_MemTotal_bytes", "gauge", "Total memory in bytes.", mem_tot_lines)
add_family("node_memory_MemAvailable_bytes", "gauge", "Available memory in bytes.", mem_avail_lines)

# 4. Node network (ens3 + lo)
rx_lines, tx_lines, rx_drop_lines, tx_drop_lines = [], [], [], []
for node in nodes:
    for dev in ("ens3", "lo"):
        rx, tx, rxd, txd = 5_000_000.0, 4_200_000.0, 0.0, 0.0
        for ts in timestamps:
            inc = incident_factor(ts) if (node == incident_node and dev == "ens3") else 0.0
            rx += (1_800_000.0 if dev == "ens3" else 120_000.0) * (1.0 + 2.2 * inc)
            tx += (1_500_000.0 if dev == "ens3" else 120_000.0) * (1.0 + 1.9 * inc)
            rxd += (4.0 * inc) if dev == "ens3" else 0.0
            txd += (1.5 * inc) if dev == "ens3" else 0.0
            rx_lines.append(f'node_network_receive_bytes_total{{device="{dev}",instance="{node}",job="node-exporter"}} {rx:.0f} {ts}')
            tx_lines.append(f'node_network_transmit_bytes_total{{device="{dev}",instance="{node}",job="node-exporter"}} {tx:.0f} {ts}')
            rx_drop_lines.append(f'node_network_receive_drop_total{{device="{dev}",instance="{node}",job="node-exporter"}} {rxd:.0f} {ts}')
            tx_drop_lines.append(f'node_network_transmit_drop_total{{device="{dev}",instance="{node}",job="node-exporter"}} {txd:.0f} {ts}')
add_family("node_network_receive_bytes_total", "counter", "Network bytes received.", rx_lines)
add_family("node_network_transmit_bytes_total", "counter", "Network bytes transmitted.", tx_lines)
add_family("node_network_receive_drop_total", "counter", "Network receive packets dropped.", rx_drop_lines)
add_family("node_network_transmit_drop_total", "counter", "Network transmit packets dropped.", tx_drop_lines)

# 5. Node disk write latency & IO time (nvme0n1)
wtime_lines, wcomp_lines, iotime_lines, ioweight_lines = [], [], [], []
last_disk: dict[str, tuple[float, float]] = {}
for node in nodes:
    wt, wc, iot, iow = 120.0, 40_000.0, 95.0, 140.0
    for ts in timestamps:
        inc = incident_factor(ts) if node == incident_node else 0.0
        writes_in_step = 450.0 + 600.0 * inc
        latency_sec = 0.0018 + (0.048 * inc)
        wc += writes_in_step
        wt += writes_in_step * latency_sec
        iot += (0.08 + 0.78 * inc) * step
        iow += (0.12 + 2.40 * inc) * step
        wtime_lines.append(f'node_disk_write_time_seconds_total{{device="nvme0n1",instance="{node}",job="node-exporter"}} {wt:.4f} {ts}')
        wcomp_lines.append(f'node_disk_writes_completed_total{{device="nvme0n1",instance="{node}",job="node-exporter"}} {wc:.0f} {ts}')
        iotime_lines.append(f'node_disk_io_time_seconds_total{{device="nvme0n1",instance="{node}",job="node-exporter"}} {iot:.4f} {ts}')
        ioweight_lines.append(f'node_disk_io_time_weighted_seconds_total{{device="nvme0n1",instance="{node}",job="node-exporter"}} {iow:.4f} {ts}')
    last_disk[node] = (wt, wc)
add_family("node_disk_write_time_seconds_total", "counter", "Disk write time seconds.", wtime_lines)
add_family("node_disk_writes_completed_total", "counter", "Disk writes completed.", wcomp_lines)
add_family("node_disk_io_time_seconds_total", "counter", "Disk IO time seconds.", iotime_lines)
add_family("node_disk_io_time_weighted_seconds_total", "counter", "Weighted disk IO time seconds.", ioweight_lines)

# 6. Pre-evaluated node-exporter recording rules for historical blocks
rule_defs = {
    "instance:node_cpu_utilisation:rate1m": lambda inc: 0.26 + 0.65 * inc,
    "instance:node_memory_utilisation:ratio": lambda inc: 0.48 + 0.41 * inc,
    "instance:node_network_receive_bytes_excluding_lo:rate1m": lambda inc: 60_000.0 * (1.0 + 2.2 * inc),
    "instance:node_network_transmit_bytes_excluding_lo:rate1m": lambda inc: 50_000.0 * (1.0 + 1.9 * inc),
    "instance:node_network_receive_drop_excluding_lo:rate1m": lambda inc: (4.0 / 30.0) * inc,
    "instance:node_network_transmit_drop_excluding_lo:rate1m": lambda inc: (1.5 / 30.0) * inc,
}
for rname, fn in rule_defs.items():
    rlines = []
    for node in nodes:
        for ts in timestamps:
            inc = incident_factor(ts) if node == incident_node else 0.0
            rlines.append(f'{rname}{{instance="{node}"}} {fn(inc):.5f} {ts}')
    add_family(rname, "gauge", f"Recording rule {rname}.", rlines)

for rname, fn in (
    ("instance_device:node_disk_io_time_seconds:rate1m", lambda inc: 0.08 + 0.78 * inc),
    ("instance_device:node_disk_io_time_weighted_seconds:rate1m", lambda inc: 0.12 + 2.40 * inc),
):
    rlines = []
    for node in nodes:
        for ts in timestamps:
            inc = incident_factor(ts) if node == incident_node else 0.0
            rlines.append(f'{rname}{{device="nvme0n1",instance="{node}"}} {fn(inc):.5f} {ts}')
    add_family(rname, "gauge", f"Recording rule {rname}.", rlines)

# 7. Pod CPU, Pod Memory & kube_pod_container_resource_limits
pods = [
    ("openshift-etcd", "etcd-worker-1", "etcd", 2.0, 4 * 1024**3, True),
    ("openshift-ovn-kubernetes", "ovnkube-node-7xk2p", "ovnkube-controller", 1.0, 2 * 1024**3, True),
    ("prod-payments", "checkout-api-7d9f8-abc", "api", 1.5, 1 * 1024**3, False),
]
pcpu_lines, pmem_lines, plim_lines = [], [], []
last_pcpu: dict[tuple[str, str], float] = {}
for ns, pod, ctr, cpu_lim, mem_lim, affected in pods:
    cpu_cum = 400.0
    for ts in timestamps:
        inc = incident_factor(ts) if affected else 0.0
        cores_used = cpu_lim * (0.28 + (0.66 * inc))
        mem_used = int(mem_lim * (0.42 + (0.51 * inc)))
        cpu_cum += cores_used * step
        pcpu_lines.append(
            f'container_cpu_usage_seconds_total{{container="{ctr}",namespace="{ns}",pod="{pod}"}} {cpu_cum:.3f} {ts}'
        )
        pmem_lines.append(
            f'container_memory_working_set_bytes{{container="{ctr}",namespace="{ns}",pod="{pod}"}} {mem_used} {ts}'
        )
        plim_lines.append(
            f'kube_pod_container_resource_limits{{container="{ctr}",namespace="{ns}",pod="{pod}",resource="cpu"}} {cpu_lim:.2f} {ts}'
        )
        plim_lines.append(
            f'kube_pod_container_resource_limits{{container="{ctr}",namespace="{ns}",pod="{pod}",resource="memory"}} {mem_lim} {ts}'
        )
    last_pcpu[(ns, pod)] = cpu_cum
add_family("container_cpu_usage_seconds_total", "counter", "Cumulative container CPU seconds consumed.", pcpu_lines)
add_family("container_memory_working_set_bytes", "gauge", "Container working set memory in bytes.", pmem_lines)
add_family("kube_pod_container_resource_limits", "gauge", "Container resource limit.", plim_lines)

# 8. KubeVirt VM filesystem metrics
vm_used_lines, vm_cap_lines = [], []
for mount, cap_gb, base_ratio in (("/", 40, 0.55), ("/var/lib/pgsql", 200, 0.72)):
    cap = cap_gb * 1024**3
    for ts in timestamps:
        inc = incident_factor(ts) if mount == "/var/lib/pgsql" else 0.0
        used = int(cap * min(0.97, base_ratio + 0.19 * inc))
        vm_used_lines.append(
            f'kubevirt_vmi_filesystem_used_bytes{{mount_point="{mount}",name="rhel9-db-01",namespace="virt-workloads"}} {used} {ts}'
        )
        vm_cap_lines.append(
            f'kubevirt_vmi_filesystem_capacity_bytes{{mount_point="{mount}",name="rhel9-db-01",namespace="virt-workloads"}} {cap} {ts}'
        )
add_family("kubevirt_vmi_filesystem_used_bytes", "gauge", "KubeVirt VMI filesystem used bytes.", vm_used_lines)
add_family("kubevirt_vmi_filesystem_capacity_bytes", "gauge", "KubeVirt VMI filesystem capacity bytes.", vm_cap_lines)

om_path = work / "backfill.om"
with om_path.open("w", encoding="utf-8") as f:
    for name, mtype, help_text, lines in families:
        f.write(f"# HELP {name} {help_text}\n")
        f.write(f"# TYPE {name} {mtype}\n")
        for line in lines:
            f.write(line + "\n")
    f.write("# EOF\n")

# Write live exporter script continuing from end_ts counter baselines so WAL + chunks_head + metadata join seamlessly.
w0_idle = last_cpu[("worker-0.example.com:9100", "0", "idle")]
w0_iow = last_cpu[("worker-0.example.com:9100", "0", "iowait")]
w1_idle = last_cpu[("worker-1.example.com:9100", "0", "idle")]
w1_iow = last_cpu[("worker-1.example.com:9100", "0", "iowait")]
w1_wt, w1_wc = last_disk["worker-1.example.com:9100"]
etcd_cpu = last_pcpu[("openshift-etcd", "etcd-worker-1")]
pay_cpu = last_pcpu[("prod-payments", "checkout-api-7d9f8-abc")]

(work / "exporter.py").write_text(f'''import http.server, time

END_TS = {end_ts}
SCRAPES = 0

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        global SCRAPES
        SCRAPES += 1
        dt = max(1.0, time.time() - END_TS)
        cpu_rows = []
        for cpu in ("0", "1", "2", "3"):
            cpu_rows.append(f'node_cpu_seconds_total{{{{cpu="{{cpu}}",instance="worker-0.example.com:9100",job="node-exporter",mode="idle"}}}} {{{w0_idle} + dt * 0.74:.3f}}')
            cpu_rows.append(f'node_cpu_seconds_total{{{{cpu="{{cpu}}",instance="worker-0.example.com:9100",job="node-exporter",mode="iowait"}}}} {{{w0_iow} + dt * 0.02:.3f}}')
            cpu_rows.append(f'node_cpu_seconds_total{{{{cpu="{{cpu}}",instance="worker-1.example.com:9100",job="node-exporter",mode="idle"}}}} {{{w1_idle} + dt * 0.74:.3f}}')
            cpu_rows.append(f'node_cpu_seconds_total{{{{cpu="{{cpu}}",instance="worker-1.example.com:9100",job="node-exporter",mode="iowait"}}}} {{{w1_iow} + dt * 0.02:.3f}}')
        cpu_block = "\\n".join(cpu_rows)
        body = f"""# HELP up Target health.
# TYPE up gauge
up{{{{instance="worker-0.example.com:9100",job="node-exporter"}}}} 1
up{{{{instance="worker-1.example.com:9100",job="node-exporter"}}}} 1
# HELP node_cpu_seconds_total Seconds the CPUs spent in each mode.
# TYPE node_cpu_seconds_total counter
{{cpu_block}}
# HELP node_load1 node_load1 load average.
# TYPE node_load1 gauge
node_load1{{{{instance="worker-0.example.com:9100",job="node-exporter"}}}} 0.90
node_load1{{{{instance="worker-1.example.com:9100",job="node-exporter"}}}} 0.95
# HELP node_load5 node_load5 load average.
# TYPE node_load5 gauge
node_load5{{{{instance="worker-0.example.com:9100",job="node-exporter"}}}} 0.85
node_load5{{{{instance="worker-1.example.com:9100",job="node-exporter"}}}} 0.90
# HELP node_load15 node_load15 load average.
# TYPE node_load15 gauge
node_load15{{{{instance="worker-0.example.com:9100",job="node-exporter"}}}} 0.80
node_load15{{{{instance="worker-1.example.com:9100",job="node-exporter"}}}} 0.85
# HELP node_memory_MemTotal_bytes Total memory in bytes.
# TYPE node_memory_MemTotal_bytes gauge
node_memory_MemTotal_bytes{{{{instance="worker-0.example.com:9100",job="node-exporter"}}}} 34359738368
node_memory_MemTotal_bytes{{{{instance="worker-1.example.com:9100",job="node-exporter"}}}} 34359738368
# HELP node_memory_MemAvailable_bytes Available memory in bytes.
# TYPE node_memory_MemAvailable_bytes gauge
node_memory_MemAvailable_bytes{{{{instance="worker-0.example.com:9100",job="node-exporter"}}}} 17867063951
node_memory_MemAvailable_bytes{{{{instance="worker-1.example.com:9100",job="node-exporter"}}}} 17867063951
# HELP node_disk_write_time_seconds_total Disk write time seconds.
# TYPE node_disk_write_time_seconds_total counter
node_disk_write_time_seconds_total{{{{device="nvme0n1",instance="worker-1.example.com:9100",job="node-exporter"}}}} {{{w1_wt} + dt * 0.027:.4f}}
# HELP node_disk_writes_completed_total Disk writes completed.
# TYPE node_disk_writes_completed_total counter
node_disk_writes_completed_total{{{{device="nvme0n1",instance="worker-1.example.com:9100",job="node-exporter"}}}} {{{int(w1_wc)} + int(dt * 15)}}
# HELP container_cpu_usage_seconds_total Cumulative container CPU seconds consumed.
# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{{{{container="etcd",namespace="openshift-etcd",pod="etcd-worker-1"}}}} {{{etcd_cpu} + dt * 0.56:.3f}}
container_cpu_usage_seconds_total{{{{container="api",namespace="prod-payments",pod="checkout-api-7d9f8-abc"}}}} {{{pay_cpu} + dt * 0.42:.3f}}
# HELP container_memory_working_set_bytes Container working set memory in bytes.
# TYPE container_memory_working_set_bytes gauge
container_memory_working_set_bytes{{{{container="etcd",namespace="openshift-etcd",pod="etcd-worker-1"}}}} 1803886264
container_memory_working_set_bytes{{{{container="api",namespace="prod-payments",pod="checkout-api-7d9f8-abc"}}}} 450971566
# HELP kube_pod_container_resource_limits Container resource limit.
# TYPE kube_pod_container_resource_limits gauge
kube_pod_container_resource_limits{{{{container="etcd",namespace="openshift-etcd",pod="etcd-worker-1",resource="cpu"}}}} 2.0
kube_pod_container_resource_limits{{{{container="etcd",namespace="openshift-etcd",pod="etcd-worker-1",resource="memory"}}}} 4294967296
kube_pod_container_resource_limits{{{{container="api",namespace="prod-payments",pod="checkout-api-7d9f8-abc",resource="cpu"}}}} 1.5
kube_pod_container_resource_limits{{{{container="api",namespace="prod-payments",pod="checkout-api-7d9f8-abc",resource="memory"}}}} 1073741824
# HELP kubevirt_vmi_filesystem_used_bytes KubeVirt VMI filesystem used bytes.
# TYPE kubevirt_vmi_filesystem_used_bytes gauge
kubevirt_vmi_filesystem_used_bytes{{{{mount_point="/var/lib/pgsql",name="rhel9-db-01",namespace="virt-workloads"}}}} 154618822656
# HELP kubevirt_vmi_filesystem_capacity_bytes KubeVirt VMI filesystem capacity bytes.
# TYPE kubevirt_vmi_filesystem_capacity_bytes gauge
kubevirt_vmi_filesystem_capacity_bytes{{{{mount_point="/var/lib/pgsql",name="rhel9-db-01",namespace="virt-workloads"}}}} 214748364800
""".encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass

http.server.HTTPServer(("127.0.0.1", 9101), Handler).serve_forever()
''', encoding="utf-8")

(work / "rules.yml").write_text('''groups:
  - name: openshift-node.rules
    interval: 500ms
    rules:
      - record: instance:node_load1_per_cpu:ratio
        expr: node_load1 / count without (cpu, mode) (node_cpu_seconds_total{mode="idle"})
      - record: instance:node_memory_utilisation:ratio
        expr: 1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes
      - record: instance:node_cpu_utilisation:rate1m
        expr: 1 - avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[1m]))
''', encoding="utf-8")

(work / "prometheus.yml").write_text('''global:
  scrape_interval: 100ms
  evaluation_interval: 500ms

rule_files:
  - /work/rules.yml

scrape_configs:
  - job_name: cluster-metrics
    honor_labels: true
    static_configs:
      - targets: ["127.0.0.1:9101"]
''', encoding="utf-8")
PYEOF

echo "==> [2/5] Creating compacted 2h ULID blocks with promtool (${PROM_IMAGE})..."
"${RUNTIME}" run --rm \
  --user "$(id -u):$(id -g)" \
  -v "${WORK_DIR}:/work" \
  --entrypoint promtool \
  "${PROM_IMAGE}" \
  tsdb create-blocks-from openmetrics /work/backfill.om /work/prometheus

"${RUNTIME}" run --rm \
  --user "$(id -u):$(id -g)" \
  -v "${WORK_DIR}:/work" \
  --entrypoint promtool \
  "${PROM_IMAGE}" \
  tsdb list /work/prometheus

echo "==> [3/5] Running live Prometheus for 18s (100ms scrape) to populate wal/, chunks_head/, metadata, and lock..."
EXPORTER_CID="$("${RUNTIME}" run -d --rm -v "${WORK_DIR}:/work:ro" "${PY_IMAGE}" python3 /work/exporter.py)"
PROM_CID="$("${RUNTIME}" run -d --rm \
  --user "$(id -u):$(id -g)" \
  --network "container:${EXPORTER_CID}" \
  -v "${WORK_DIR}:/work" \
  "${PROM_IMAGE}" \
  --config.file=/work/prometheus.yml \
  --storage.tsdb.path=/work/prometheus \
  --storage.tsdb.wal-compression \
  --enable-feature=metadata-wal-records)"

sleep 18

echo "==> [4/5] Archiving live TSDB mid-flight (with active lock + WAL + chunks_head)..."
CLEAN_TAR="${OUT_DIR}/openshift-tsdb-clean.tar.gz"
COPYFILE_DISABLE=1 tar -C "${WORK_DIR}" -czf "${CLEAN_TAR}" prometheus

"${RUNTIME}" rm -f "${PROM_CID}" "${EXPORTER_CID}" >/dev/null 2>&1 || true
PROM_CID=""
EXPORTER_CID=""

echo "==> [5/5] Creating damaged twin (truncated block index + torn WAL tail)..."
DAMAGED_STAGE="${WORK_DIR}/damaged"
mkdir -p "${DAMAGED_STAGE}"
cp -R "${WORK_DIR}/prometheus" "${DAMAGED_STAGE}/prometheus"

python3 - "${DAMAGED_STAGE}/prometheus" <<'PYEOF'
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
blocks = sorted(p for p in root.iterdir() if p.is_dir() and (p / "meta.json").exists())
if blocks:
    idx = blocks[0] / "index"
    data = idx.read_bytes()
    idx.write_bytes(data[: len(data) // 2])
    print(f"    corrupted block index: {blocks[0].name}/index ({len(data)} -> {len(data)//2} bytes)")

wal_dir = root / "wal"
segments = sorted(p for p in wal_dir.iterdir() if p.is_file() and p.name.isdigit())
if segments:
    seg = segments[-1]
    raw = bytearray(seg.read_bytes())
    if len(raw) > 32:
        raw[-1] ^= 0xFF
        seg.write_bytes(bytes(raw))
        print(f"    corrupted WAL tail byte: wal/{seg.name} (offset {len(raw)-1})")
PYEOF

DAMAGED_TAR="${OUT_DIR}/openshift-tsdb-damaged.tar.gz"
COPYFILE_DISABLE=1 tar -C "${DAMAGED_STAGE}" -czf "${DAMAGED_TAR}" prometheus

cat <<EOF

Done! Fixtures written to:
  1. ${CLEAN_TAR}
  2. ${DAMAGED_TAR}

Smoke-test commands for prometheus-tsdb-reader:
  prometheus-tsdb-reader --dump "${CLEAN_TAR}" --scratch /tmp/tsdb-work info
  prometheus-tsdb-reader --dump "${CLEAN_TAR}" --scratch /tmp/tsdb-work query metadata
  prometheus-tsdb-reader --dump "${CLEAN_TAR}" --scratch /tmp/tsdb-work named node-cpu --instance worker-1.example.com:9100 --start 4h --window 5m
  prometheus-tsdb-reader --dump "${CLEAN_TAR}" --scratch /tmp/tsdb-work named node-disk-write-latency --instance worker-1.example.com:9100 --start 4h --window 5m
  prometheus-tsdb-reader --dump "${CLEAN_TAR}" --scratch /tmp/tsdb-work named pod-memory --namespace openshift-etcd --start 4h --window 5m
  prometheus-tsdb-reader --dump "${CLEAN_TAR}" --scratch /tmp/tsdb-work named vm-filesystem --namespace virt-workloads --start 4h --window 5m
  prometheus-tsdb-reader --dump "${DAMAGED_TAR}" --scratch /tmp/tsdb-work-bad info
EOF
