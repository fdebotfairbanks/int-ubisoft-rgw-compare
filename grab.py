#!/usr/bin/python


import pprint
import rados
import subprocess
import json
import redis
import re
import threading
import queue
import argparse
import sys


parser = argparse.ArgumentParser(description="Script that requires --db and --poolname.")
parser.add_argument('--db', required=True, help='Path to the database or connection string')
parser.add_argument('--dbport', required=False, default=6379, help='Redis port')
parser.add_argument('--poolname', required=True, help='Name of the pool to use')
parser.add_argument('--cluster', required=True, help='Name of the ceph cluster')

args = parser.parse_args()
pool_name = args.poolname
redis_db = args.db
redis_port = args.dbport
cluster = args.cluster

r = redis.Redis(host='localhost', port=redis_port, db=redis_db)

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
            bucket_id_map[bucket_id] = bucket

    return bucket_id_map

def get_pgids(pool_name):
    """Get PG IDs for a given pool."""
    try:
        result = subprocess.run(
            ["ceph", '--cluster', cluster, "pg", "ls-by-pool", pool_name, "--format=json"],
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
            ["rados", "--cluster", cluster, "-p", pool_name, "--pgid", pgid, "ls"],
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


# multipart_pattern = re.compile(
#     r'^(?P<full_id>[a-f0-9\-]+\.\d+\.\d+)__multipart_(?P<objectname>.+?)\.\d+~(?P<postfix>[^.]+)\.(?P<number>\d+)$'
# )
# shadow_pattern = re.compile(
#     r'^(?P<full_id>[a-f0-9\-]+\.\d+\.\d+)__shadow_(?P<objectname>.+?)\.\d+~(?P<postfix>[^.]+)\.(?P<number>\d+_\d+)$'
# )

multipart_pattern = re.compile(
    r'^(?P<full_id>[a-f0-9\-]+\.\d+\.\d+)__multipart_(?P<objectname>.+)\.(?P<postfix>\d+~[^.]+)\.(?P<number>\d+)$'
)
shadow_pattern = re.compile(
    r'^(?P<full_id>[a-f0-9\-]+\.\d+\.\d+)__shadow_(?P<rest>.*?)(?P<suffix>\.\d+_\d+|_\d+)$'
)
endofshadow_pattern = re.compile(
    r'(?P<suffix>(?:_\d+|\d+_\d+))$'
)

regular_pattern = re.compile(
    r'^(?P<full_id>[a-f0-9\-]+\.\d+\.\d+)_(?P<objectname>.+)$'
)


q = queue.Queue(maxsize=1000)


def worker(q: queue.Queue):
    global processed
    while True:
        pgid = q.get()

        if pgid is None:
            break  # sentinel = stop signal
        
        try:
            print(f'do {pgid}')
            grab_objects(pgid)
            
        finally:
            q.task_done()

def grab_objects(pgid):
    objects = list_objects_in_pg(pool_name, pgid)
    for object in objects:
        print(f"Checking {object}")
        multipart_match = multipart_pattern.match(object)
        if multipart_match:
            print(f"Found multipart {object}")
            key = f'multipart:{multipart_match.groupdict()["full_id"]}__multipart_{multipart_match.groupdict()["objectname"]}.{multipart_match.groupdict()["postfix"]}'
            r.sadd(key, multipart_match.groupdict()['number'])
            continue

        shadow_match = shadow_pattern.match(object)
        if shadow_match:
            print(f"Found shadow {object}")
            key = f'shadow:{shadow_match.groupdict()["full_id"]}__shadow_{shadow_match.groupdict()["rest"]}'
            
            r.sadd(key, shadow_match.groupdict()['suffix'])
            continue
        
        regular_match = regular_pattern.match(object)
        if regular_match:
            print(f"Found regular {object}")

            key = f'object:{regular_match.groupdict()["full_id"]}_{regular_match.groupdict()["objectname"]}'
            # r.hset(key, mapping={'pgid': pgid, 'rados_object': object})
            r.hset(key, mapping={'pgid': pgid})
            continue

        print("I should not  reach here")
        exit(1)
        
    print(f"Stored {len(objects)} for pg {pgid}")    

bucket_map = build_bucket_id_map()
print("Bucket ID to Name Mapping:")
for bucket_id, name in bucket_map.items():
    print(f"{bucket_id} : {name}")
    
print("\n\n")

pgids = get_pgids(pool_name)

# Start workers
threads = []
for _ in range(16):
    t = threading.Thread(target=worker, args=(q,))
    t.start()
    threads.append(t)

    
for pgid in pgids:
    q.put(pgid)
        

# Send stop signals
for _ in threads:
    q.put(None)

# Wait for workers to finish
for t in threads:
    t.join()