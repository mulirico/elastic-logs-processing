import pandas as pd
import re

from datetime import datetime

log_file_path = "../logs/elasticsearch-0_istio-proxy.log"

log_pattern = re.compile(
    r"""\[
    (?P<timestamp>[^\]]+)
    \]\s+"(?P<method>[A-Z]+)\s+
    (?P<endpoint>[^\s]+)\s+
    HTTP/[^"]+"\s+
    (?P<status_code>\d{3})\s+-\s+via_upstream\s+-\s+"(?P<client_ip>[^"]+)"\s+
    (?P<bytes_sent>\d+)\s+
    (?P<bytes_received>\d+)\s+
    (?P<request_duration_ms>\d+)\s+
    (?P<upstream_duration_ms>\d+)\s+"[^"]+"\s+"(?P<request_id>[^"]+)"\s+
    "(?P<cluster_host>[^"]+)"\s+"(?P<node_ip>[^"]+)"
    """, re.VERBOSE
)

log_entries = []

def parse_timestamp(ts):
    if ts is None:
        return None
    ts = ts.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(ts, fmt)
        except ValueError:
            continue
    return None

with open(log_file_path, "r") as file:
    for line in file:
        if not line.startswith("["):
            continue
        match = log_pattern.search(line)
        if not match:
            continue
        entry = match.groupdict()
        
        for field in ["status_code", "bytes_sent", "bytes_received", "request_duration_ms", "upstream_duration_ms"]:
            try:
                entry[field] = int(entry[field]) if entry[field] != '-' else None
            except ValueError:
                entry[field] = None

        entry["timestamp"] = parse_timestamp(entry["timestamp"])
        entry["request_duration_sec"] = entry["request_duration_ms"] / 1000 if entry["request_duration_ms"] else None
        entry["upstream_duration_sec"] = entry["upstream_duration_ms"] / 1000 if entry["upstream_duration_ms"] else None

        log_entries.append(entry)

df = pd.DataFrame(log_entries)

if not df.empty and "endpoint" in df.columns:
    slow_queries = df[df["request_duration_sec"] > 10.0]
    slow_queries = slow_queries.sort_values(by="request_duration_sec", ascending=False)
    print(slow_queries[["timestamp", "method", "endpoint", "status_code", "request_duration_sec"]])
else:
    print("None valid entry found or invalid data formating.")
