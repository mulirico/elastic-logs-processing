import pandas as pd
import re
import logging
import os

from datetime import datetime

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('log_parser_trace.log'),
        logging.StreamHandler(),
    ],
)

log_file_path = './logs/elasticsearch-1_istio-proxy.log'
if not os.path.exists(log_file_path):
    logging.error(f'File not found in: {log_file_path}')
else:
    logging.info(f'Reading file: {log_file_path}')

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
    "(?P<cluster_host>[^"]+)"\s+"(?P<node_ip>[^"]+)"(?:\s|$)
    """,
    re.VERBOSE,
)

log_entries = []


def parse_timestamp(ts):
    if ts is None:
        return None
    ts = ts.strip()
    for fmt in ('%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ'):
        try:
            return datetime.strptime(ts, fmt)
        except ValueError as e:
            logging.error(
                'Timestamp parsing failed: %s\nException: %s',
                ts,
                e,
                exc_info=True,
            )
            continue
    return None


with open(log_file_path, 'r') as file:
    for line in file:
        if not line.startswith('['):
            logging.warning('Line does not start with "[":\n%s', line)
            continue

        try:
            match = log_pattern.search(line)
            if not match:
                logging.warning('Line does not match regex configs:\n%s', line)
                continue
            entry = match.groupdict()

            for field in [
                'status_code',
                'bytes_sent',
                'bytes_received',
                'request_duration_ms',
                'upstream_duration_ms',
            ]:
                try:
                    entry[field] = (
                        int(entry[field]) if entry[field] != '-' else None
                    )
                except ValueError as e:
                    logging.error(
                        'Error parsing line: %s\nException: %s',
                        line,
                        e,
                        exc_info=True,
                    )
                    entry[field] = None

            entry['timestamp'] = parse_timestamp(entry['timestamp'])
            entry['request_duration_sec'] = (
                entry['request_duration_ms'] / 1000
                if entry['request_duration_ms']
                else None
            )
            entry['upstream_duration_sec'] = (
                entry['upstream_duration_ms'] / 1000
                if entry['upstream_duration_ms']
                else None
            )

            log_entries.append(entry)
        except Exception as e:
            logging.critical(
                'Unhandled exception parsing line: \n%s\nException: %s',
                line.strip,
                e,
                exc_info=True,
            )

df = pd.DataFrame(log_entries)

if not df.empty and 'endpoint' in df.columns:
    slow_queries = df[df['request_duration_sec'] > 10.0]
    slow_queries = slow_queries.sort_values(
        by='request_duration_sec', ascending=False
    )
    print(
        slow_queries[
            [
                'timestamp',
                'method',
                'endpoint',
                'status_code',
                'request_duration_sec',
            ]
        ]
    )
else:
    logging.info('None valid entry found or invalid data formating.')
    print('None valid entry found or invalid data formating.')

