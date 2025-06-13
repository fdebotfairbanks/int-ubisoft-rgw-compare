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
parser.add_argument('--poolname', required=True, help='Name of the pool to use')
parser.add_argument('--cluster', required=True, help='Name of the ceph cluster')

args = parser.parse_args()
pool_name = args.poolname
redis_db = args.db
cluster = args.cluster


# Connect to the cluster
cluster = rados.Rados(conffile=f'/etc/ceph/{cluster}.conf')
cluster.connect()

ioctx = cluster.open_ioctx(pool_name)

r = redis.Redis(host='localhost', port=6379, db=redis_db)
t = time.gmtime()

def stat_object(cluster: rados.Rados, object_name: str) -> Dict:
    """Stat a single object using librados."""
    try:
        size, mtime = ioctx.stat(object_name)
        print(f"Object: {object_name} mtime: {mtime}")
    except Exception as e:
        
        return {
            "object": object_name,
            "error": str(e)
        }

def get_manifest(object_name: str) -> Dict:
    print(f"getmanifest({object_name})")
    try:
        value = ioctx.get_xattr(object_name, 'user.rgw.manifest')
        with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
            tmpfile.write(value)
            tmpfile_path = tmpfile.name


        completed = subprocess.run(
            ['ceph-dencoder', 'type', 'RGWObjManifest', 'import', tmpfile_path, 'decode', 'dump_json'],
            check=True,
            capture_output=True,
            text=True
        )
        result = json.loads(completed.stdout)
        
        regular_match = regular_pattern.match(object_name)

        if (result['end_iter']['cur_part_id'] == 0):
            r.hset(f'object:{regular_match.groupdict()["full_id"]}_{regular_match.groupdict()["objectname"]}', mapping={'mp': 0})
        else:
            r.hset(f'object:{regular_match.groupdict()["full_id"]}_{regular_match.groupdict()["objectname"]}', mapping={'mp': 1, 'mp_prefix': result['prefix'] })
            
        
            
    except subprocess.CalledProcessError as e:
        print("Decode failed with code:", e.returncode)
        print("STDERR:", e.stderr)
        print("STDOUT:", e.stdout)            
            
    except Exception as e:
        pprint.pprint(e)
        print('bah')
        

regular_pattern = re.compile(
    r'^(?P<full_id>[a-f0-9\-]+\.\d+\.\d+)_(?P<objectname>.+)$'
)


processed = 0
def worker(q: queue.Queue):
    global processed
    while True:
        object_name = q.get()
        print(f"processing {object_name}")
        if object_name is None:
            break  # sentinel = stop signal

        try:
            size, mtime = ioctx.stat(object_name)
            mtime_timestamp = str(calendar.timegm(mtime))
            
            regular_match = regular_pattern.match(object_name)
            
            print(f'object:{regular_match.groupdict()["full_id"]}_{regular_match.groupdict()["objectname"]} size={size}, mtime={mtime_timestamp}')
            r.hset(f'object:{regular_match.groupdict()["full_id"]}_{regular_match.groupdict()["objectname"]}', mapping={'mtime': mtime_timestamp})
            
            
            # Manifest
            get_manifest(object_name)
            processed += 1
            
        except rados.ObjectNotFound:
            print(f"[ERROR] {object_name}: not found")
        except Exception as e:
            print(f"[ERROR] {object_name}: {e}")
        finally:
            q.task_done()
            
        if (processed % 1000 == 0):
            print(f"processed {processed} objects")

q = queue.Queue(maxsize=1000)

# Start workers
threads = []
for _ in range(4):
    t = threading.Thread(target=worker, args=(q,))
    t.start()
    threads.append(t)

for key in r.scan_iter(match="object:*"):
    q.put(r.hget(key.decode('utf-8'), 'rados_object').decode('utf-8'))

# Send stop signals
for _ in threads:
    q.put(None)

# Wait for workers to finish
for t in threads:
    t.join()