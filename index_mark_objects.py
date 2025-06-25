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
import string
import os

parser = argparse.ArgumentParser(description="Script that requires --db and --poolname.")
parser.add_argument('--db', required=True, help='Path to the database or connection string')
parser.add_argument('--dbport', required=False, default=6379, help='Redis port')
parser.add_argument('--cluster', required=True, help='Name of the ceph cluster')

args = parser.parse_args()

redis_db = args.db
redis_port = args.dbport
cluster = args.cluster



r = redis.Redis(host='localhost', port=redis_port, db=redis_db)
t = time.gmtime()

directory = '/script/omapdata/'

pattern = re.compile(r'^(\w{8}(?:-\w{4}){3}-\w{12}\.\d+\.\d+)(?:\.\d+)?$')



def run_rgw_admin(cmd):
    """Run radosgw-admin and return parsed JSON output."""
    try:
        result = subprocess.run(
            ['radosgw-admin', '--cluster', cluster] + cmd,
            capture_output=True,
            check=True,
            text=True
        )
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {' '.join(e.cmd)}")
        print(f"Error: {e.stderr}")
        return None
    
def build_bucket_id_map():
    bucket_id_map = {}

    # Step 1: Get list of all buckets
    bucket_names = run_rgw_admin(['bucket', 'list'])
    if bucket_names is None:
        return {}

    # Step 2: For each bucket, get the bucket ID
    for bucket in bucket_names:
        stats = run_rgw_admin(['bucket', 'stats', '--bucket', bucket])
        if stats and 'id' in stats:
            bucket_id = stats['id']
            bucket_id_map[bucket_id] = {}
            bucket_id_map[bucket_id]['name'] = bucket
            bucket_id_map[bucket_id]['marker'] = stats['marker']

    return bucket_id_map

# {'88b24e22-5889-4011-88db-eeeb6fca97c3.19525.1': {'marker': '88b24e22-5889-4011-88db-eeeb6fca97c3.19525.1',
#                                                   'name': 'multipart'},

bucket_id_map = build_bucket_id_map()
#bucket_id_map = {'88b24e22-5889-4011-88db-eeeb6fca97c3.19525.1': {'marker': '88b24e22-5889-4011-88db-eeeb6fca97c3.19525.1','name': 'multipart'}}
for filename in os.listdir(directory):
    filepath = os.path.join(directory, filename)

    # Only process regular files (skip subdirs, etc.)
    if os.path.isfile(filepath):
        match = pattern.match(filename)
        if match:
            base = match.group(1)
            with open(filepath, 'r') as f:
                for line in f:
                    # process each line
                    # Check if base is mwatched in bucket mapping

                    if base in bucket_id_map:
                        object_name = f'object:{bucket_id_map[base]["marker"]}_{line.strip()}'
                        print(f"Checking for {object_name}")
                        
                        if r.exists(object_name):
                            r.hset(object_name, mapping={'f': 1})
                        else:
                            print('   - not found')
                    else:
                        print(f"Could not match index {base} with any bucket markers")