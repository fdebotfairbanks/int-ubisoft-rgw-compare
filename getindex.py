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
parser.add_argument('--poolname', required=True, help='Name of the pool to use')
parser.add_argument('--cluster', required=True, help='Name of the ceph cluster')

args = parser.parse_args()
pool_name = args.poolname
redis_db = args.db
redis_port = args.dbport
clustername = args.cluster

# Connect to the cluster
cluster = rados.Rados(conffile=f'/etc/ceph/{clustername}.conf')
cluster.connect()

ioctx = cluster.open_ioctx(pool_name)

r = redis.Redis(host='localhost', port=redis_port, db=redis_db)
t = time.gmtime()

fds = {}

def is_open(name):
    return name in fds

def open_file(name):
    print(f'opening omapdata/{name}')
    fds[name] = os.open(os.path.join("/script/omapdata/", name), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    

def get_pgids(pool_name):
    """Get PG IDs for a given pool."""
    try:
        result = subprocess.run(
            ["ceph", '--cluster', clustername, "pg", "ls-by-pool", pool_name, "--format=json"],
            check=True,
            capture_output=True,
            text=True
        )
        pg_data = json.loads(result.stdout)
        pgids = [pg["pgid"] for pg in pg_data["pg_stats"]]
        return pgids
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to get PGs: {e.stderr}")
        return []

def list_objects_in_pg(pool_name, pgid):
    """Run 'rados -p pool --pgid pgid ls' and return list of objects."""
    try:
        result = subprocess.run(
            ["rados", "--cluster", clustername, "-p", pool_name, "--pgid", pgid, "ls"],
            check=True,
            capture_output=True,
            text=True
        )
        objects = result.stdout.strip().splitlines()
        return objects
    except subprocess.CalledProcessError as e:
        if "error getting pg" in e.stderr.lower():
            return []  # PG not on this host
        print(f"[ERROR] PG {pgid}: {e.stderr}")
        return []

def is_first_char_printable(value):
    value_str = str(value)
    return bool(re.match(r'[\x20-\x7E]', value_str))

def is_all_printable(byte_data):
    printable_bytes = set(bytes(string.printable, 'ascii'))
    return all(b in printable_bytes for b in byte_data)

pattern = re.compile(r'^\.dir\.(\w{8}(?:-\w{4}){3}-\w{12}\.\d+\.\d+)(?:\.\d+)?$')

def grab_objects(pgid):
    objects = list_objects_in_pg(pool_name, pgid)
    
    for object in objects:
        
        match = pattern.match(object)
        
        base = match.group(1)
        
        if not is_open(base):
            open_file(base)
            
        result = subprocess.run(
            ["rados", "--cluster", clustername, "-p", pool_name, "--pgid", pgid, "listomapkeys", object],
            check=True,
            capture_output=True,
            text=False
        )
        # pprint.pprint(result)
        omaps = result.stdout.strip().splitlines()
        
        for omap_key in omaps:
            if is_all_printable(omap_key):
                os.write(fds[base], omap_key + b"\n")
                pprint.pprint(omap_key)

   

                 
pgids = get_pgids(pool_name)

for pgid in pgids:
    grab_objects(pgid)
    
    
for fd in fds.values():
    os.close(fd)