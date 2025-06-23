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

log_file_path = './logs/elasticsearch-0_elasticsearch.log'
if not os.path.exists(log_file_path):
    logging.error(f'File not found in: {log_file_path}')
else:
    logging.info(f'Reading file: {log_file_path}')

log_pattern = re.compile(
    r"""
    ^\[(?P<timestamp>[\d\-T:,]+)\]
    \[(?P<level>[A-Z]+)\s*\]
    \[(?P<source>[^\]]+)\]
    \s+\[(?P<node>[^\]]+)\]
    \s+\[(?P<index>[^\]]+)\]\[(?P<shard>\d+)\]
    \s+took\[(?P<duration>[\d.]+)(?P<unit>ms|s)\],
    \s+took_millis\[(?P<duration_ms>\d+)\],     
    .*?source\[(?P<query_type>\{.*?)(?:\]|\Z)
    """,
    re.VERBOSE,
)

log_entries = []

with open(log_file_path, 'r') as file:
    try:
        for line in file:
            match = log_pattern.search(line)
            if match:
                entry = match.groupdict()
                log_entries.append(entry)
            else:
                logging.warning('Line does not match regex configs:\n%s', line)
    except Exception as e:
        logging.critical(
            'Unhandled exception parsing line: \n%s\nException: %s',
            line.strip,
            e,
            exc_info=True,
        )

df = pd.DataFrame(log_entries)

df['duration_ms'] = pd.to_numeric(df['duration_ms'])

if not df.empty and 'source' in df.columns:
    slow_queries = df[df['duration_ms'] > 1000]
    slow_queries = slow_queries.sort_values(
        by='duration_ms', ascending=False
    )
    print(
        slow_queries[
            [
                'timestamp',
                'level',
                'source',
                'index',
                'duration_ms',
                'query_type',
            ]
        ]
    )
else:
    logging.info('None valid entry found or invalid data formating.')
    print('None valid entry found or invalid data formating.')

