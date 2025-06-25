#!/usr/bin/python


import pprint
import rados
import subprocess
import json
import redis

import rados
import concurrent.futures
from typing import List, Dict
import threading
import queue
import time
import calendar
import re
import tempfile
import argparse
import sys

parser = argparse.ArgumentParser(description="Script that requires --db and --poolname.")
parser.add_argument('--db', required=True, help='Path to the database or connection string')
parser.add_argument('--dbport', required=False, default=6379, help='Redis port')

args = parser.parse_args()
redis_db = args.db
redis_port = args.dbport


r = redis.Redis(host='localhost', port=redis_port, db=redis_db)
t = time.gmtime()

pipeline = r.pipeline()

BATCH_SIZE = 1000  # or tune this depending on your memory and latency needs

batch = []

for key in r.scan_iter(match="object:*"):
    results = pipeline.hgetall(key)
    batch.append(key)

    if len(batch) >= BATCH_SIZE:
        pipe = r.pipeline()
        for k in batch:
            pipe.hgetall(k)
        results = pipe.execute()

        for k, data in zip(batch, results):
            # Do something with each key and its hash
            if not b'f' in data:
                print(f'object without index entry: {key.decode("utf-8")}')
                
        
        batch.clear()  # reset for next round
        
# Final partial batch (if any)
if batch:
    pipe = r.pipeline()
    for k in batch:
        pipe.hgetall(k)
    results = pipe.execute()
    for k, data in zip(batch, results):
        if not b'f' in data:
            print(f'object without index entry: {key.decode("utf-8")}')
