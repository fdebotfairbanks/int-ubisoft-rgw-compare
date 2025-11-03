#!/usr/bin/python


import pprint
import rados
import subprocess
import json
import redis

import rados
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor
from typing import List, Dict
import threading
import queue
import time
import calendar
import re
import tempfile
import argparse
import sys
import psycopg2
import logging
from psycopg2.extras import DictCursor
import traceback
import faulthandler, signal
faulthandler.register(signal.SIGUSR1)


parser = argparse.ArgumentParser(description="Script that requires --db and --poolname.")
parser.add_argument('--dbport', required=False, default=6379, help='Redis port')
parser.add_argument('--poolname', required=True, help='Name of the pool to use')
parser.add_argument('--cluster', required=True, help='Name of the ceph cluster')


# Configuratie
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "mydb"
DB_USER = "myuser"
DB_PASS = "mypassword"
batch_size = 1000

LOG_LEVEL = logging.DEBUG

args = parser.parse_args()
pool_name = args.poolname
cluster_name = args.cluster

pg_stats = {}

stats = {}
stats['total'] = {}
stats['done'] = {}
stats['total']['pg_num'] = 0
stats['total']['objects'] = 0
stats['done']['pg_num'] = 0
stats['done']['objects'] = 0

stats['previous'] = {}
stats['previous']['objects'] = 0


pool = redis.ConnectionPool(host="localhost", port=6379, db=0, max_connections=128)
r = redis.Redis(connection_pool=pool)


# Connect to the cluster
cluster = rados.Rados(conffile=f'/etc/ceph/{cluster_name}.conf')
cluster.connect()


# Configure logger
logger = logging.getLogger("myapp")
logger.setLevel(LOG_LEVEL)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(LOG_LEVEL)
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] pid=%(process)d #%(lineno)d %(message)s",  # format with timestamp
    datefmt="%Y-%m-%d %H:%M:%S"                # timestamp format
)
handler.setFormatter(formatter)

logger.addHandler(handler)

logger.debug("LOG!")


conn_global = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS
) 

cur_global = conn_global.cursor()

# db changes
sql_shadow = "ALTER TABLE shadow ADD COLUMN IF NOT EXISTS mtime TIMESTAMP"
cur_global.execute(sql_shadow)
conn_global.commit()


# Bucketmapping
bucket_map = {}


# regexps
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



def get_pgids(pool_name):
    """Get PG IDs for a given pool."""
    logger.info(f"get_pgids({pool_name})")
    try:
        result = subprocess.run(
            ["ceph", '--cluster', cluster_name, "pg", "ls-by-pool", pool_name, "--format=json"],
            check=True,
            capture_output=True,
            text=True
        )
        pg_data = json.loads(result.stdout)
        
        pgids = [pg["pgid"] for pg in pg_data["pg_stats"]]
        
        for pg in pg_data["pg_stats"]:
            pg_stats[pg["pgid"]] = {"objects": pg["stat_sum"]["num_objects"]}
            stats['total']['objects'] = stats['total']['objects'] + pg["stat_sum"]["num_objects"]
            
        stats['total']['pg_num'] = len(pgids)
        
        
        return pgids
    except subprocess.CalledProcessError as e:
        logger.error(f"[ERROR] Failed to get PGs: {e.stderr}")
        return []


def stat_object(cluster: rados.Rados, object_name: str) -> Dict:
    """Stat a single object using librados."""
    try:
        size, mtime = ioctx.stat(object_name)
        logger.debug(f"Object: {object_name} mtime: {mtime}")
    except Exception as e:
        
        return {
            "object": object_name,
            "error": str(e)
        }

def get_manifest(object_name: str) -> Dict:
    logger.debug(f"getmanifest({object_name})")
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
        logger.info("Decode failed with code:", e.returncode)
        logger.info("STDERR:", e.stderr)
        logger.info("STDOUT:", e.stdout)            
            
    except Exception as e:
        logger.info(f'bah: {e}')
        

def get_mtime(object_name, conn, cluster, redis_pipe, bucket_id):
    logger.debug(f"processing {object_name}")

    try:
        ioctx = cluster.open_ioctx(pool_name)
        size, mtime = ioctx.stat(object_name)
        mtime_timestamp = str(calendar.timegm(mtime))
        
        regular_match = regular_pattern.match(object_name)
        
        logger.debug(f'object:{regular_match.groupdict()["full_id"]}_{regular_match.groupdict()["objectname"]} size={size}, mtime={mtime_timestamp}')
        
        # INSERT INTO DB please
        
        redis_pipe.lpush("queue:object_mtime", json.dumps({'bucket_id': bucket_id, 
                                        'object': regular_match.groupdict()["objectname"], 
                                        'pg_id': pgid,
                                        'mtime': mtime_timestamp
                                        }))

        # Manifest
        # get_manifest(object_name)
        
    except rados.ObjectNotFound:
        logger.info(f"[ERROR] {object_name}: not found")
    except Exception as e:
        logger.info(f"[ERROR] {object_name}: {e}")

def get_bucket_prefix(bucket_id):
    if bucket_id in bucket_map:
        return bucket_map[bucket_id]
    
    
    with conn_global.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT * FROM buckets WHERE id = %s", (bucket_id,))
        row = cur.fetchone()

        pprint.pprint(row)

        bucket_map[row['id']] = row['bucket_marker']
        logger.debug(f"Returning marker {row['bucket_marker']}")
        return row['bucket_marker']

    return  None 

def worker_mtimes(stop_event):
    logger.info("worker_mtimes() running")
    
    # Connectie openen
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    ) 
    # Cursor aanmaken
    cur = conn.cursor()

    count = 0
    
    while not stop_event.is_set():
        try:
            logger.info(f"worker_mtimes()  doing stuff")
            # Shadow
            while True:
                msg = r.rpop("queue:object_mtime")  # 1 = max aantal keys
                
                if msg is not None:
                    logger.debug(f"Ontvangen: {msg}")
                    data = json.loads(msg.decode('utf-8'))
                    cur.execute("UPDATE objects SET mtime=to_timestamp(%s) WHERE bucket_id=%s AND pg_id=%s AND object=%s", 
                                (data['mtime'], data['bucket_id'], data['pg_id'], data['object'], )
                                )
                    
                    count += 1
                    
                    if count % 10:
                        conn.commit()

                else:
                    conn.commit()
                    logger.info("Geen messages (meer)")
                    break

        except Exception as e:
            logger.error(f"[stats] Error: {e}")
        stop_event.wait(1)   # sleep 5s, but wake early if stop_event is set



def get_and_process_objects(pgid):
    logger.info(f"get_and_process_objects({pgid})")
    count = 0
    try:
        # Connect to the cluster
        cluster = rados.Rados(conffile=f'/etc/ceph/{cluster_name}.conf')
        cluster.connect()

        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )

        redis_pipe = r.pipeline(transaction=False)

        with conn.cursor(name='streaming_cursor', cursor_factory=DictCursor) as cur:
            cur.itersize = batch_size
            cur.execute("SELECT * FROM objects WHERE pg_id = %s AND mtime IS NULL", (pgid,))

            batch_num = 0
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                for i, row in enumerate(rows):
                    # Process rows here
                    logger.info(f"Processing row {i} of batch {batch_num} for pgid={pgid} => {row['object']}")
                    # Example: do something with row

                    bucket_prefix = get_bucket_prefix(row['bucket_id'])
                    rados_object_name = f"{bucket_prefix}_{row['object']}"
                    get_mtime(rados_object_name, conn, cluster, redis_pipe, row['bucket_id'])
                    
                    if count % 1000 == 0:
                        redis_pipe.execute()
                        
                    count += 1

                batch_num += 1
                
        # Loop shadow objects

        with conn.cursor(name='streaming_cursor', cursor_factory=DictCursor) as cur:
            cur.itersize = batch_size
            cur.execute("SELECT * FROM shadow WHERE pg_id[0] = %s AND mtime IS NULL", (pgid,))

            batch_num = 0
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                for i, row in enumerate(rows):
                    # Process rows here
                    logger.info(f"Processing row {i} of batch {batch_num} for pgid={pgid} => {row['object']}")
                    # Example: do something with row

                    bucket_prefix = get_bucket_prefix(row['bucket_id'])
                    rados_object_name = f"{bucket_prefix}_{row['object']}"
                    get_mtime(rados_object_name, conn, cluster, redis_pipe, row['bucket_id'])
                    
                    if count % 1000 == 0:
                        redis_pipe.execute()
                        
                    count += 1

                batch_num += 1

    except Exception:
        logger.exception(f"Exception in get_and_process_objects({pgid})")
        raise
    finally:
        redis_pipe.execute()
        if conn:
            conn.close()

    logger.info(f"Finish pg {pgid}")


pgids = get_pgids(pool_name)


stop_event = threading.Event()
# Start stats thread
thread_mtime = threading.Thread(target=worker_mtimes, args=(stop_event,))
thread_mtime.start()


logger.info(f"Starting executing")
for pgid in pgids:
    get_and_process_objects(pgid)
# with ProcessPoolExecutor(max_workers=8) as executor:
#     executor.map(get_and_process_objects, pgids)

time.sleep(10)
# Signal stats thread to stop
stop_event.set()
thread_mtime.join()

logger.info("done")
