#!/usr/bin/env python3

import argparse
import os
import rados
import logging
import subprocess
import sys
import json
import psycopg2
from psycopg2.extras import DictCursor
from concurrent.futures import ProcessPoolExecutor
import traceback
import pprint
from itertools import repeat
import hashlib
from multiprocessing import Process, Value


bucket_map = {}

# Configuratie
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "mydb"
DB_USER = "myuser"
DB_PASS = "mypassword"

LOG_LEVEL = logging.INFO

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

global_count = Value('i', 0)

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
        logger.error(f"[ERROR] Failed to get PGs: {e.stderr}")
        return []


def ceph_decode(path, typename):
    cmd = ["ceph-dencoder", "type", typename, "import", path, "decode", "dump_json"]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return json.loads(result.stdout)


def search_objects(pgid, cluster, pool_name, marker):
    try:
        batch_size = 1000
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        # Connect to the cluster
        cluster = rados.Rados(conffile=f'/etc/ceph/{cluster}.conf')
        cluster.connect()
        
        cnt = 0
        with open(f"/script/darkdata/{pgid}", "w") as f:
            with cluster.open_ioctx(pool_name) as ioctx:
                with conn.cursor(name='streaming_cursor', cursor_factory=DictCursor) as cur:
                    cur.itersize = batch_size
                    cur.execute("""SELECT * FROM objects WHERE pg_id = %s AND 
                                                               mtime IS NULL AND
                                                               bucket_id = %s
                                                               """, (pgid,bucket_map[marker]))
                    while True:
                        rows = cur.fetchmany(batch_size)
                        if not rows:
                            break
                        for i, row in enumerate(rows):
                            rados_name = f"{get_bucket_prefix(row['bucket_id'])}_{row['object']}"
                            rados_name_hashed = hashlib.sha256(rados_name.encode()).hexdigest()
                            logger.debug(f"pgid: {row['pg_id']} object: {row['object']} (Rados: {rados_name}) tmp: {rados_name_hashed}")
                            
                            data = ioctx.get_xattr(rados_name, "user.rgw.manifest")

                            # Wegschrijven naar bestand
                            with open(f"/script/tmp/{rados_name_hashed}", 'wb') as f2:
                                f2.write(data)                        
                            
                            data_decoded = ceph_decode(f"/script/tmp/{rados_name_hashed}", "RGWObjManifest")

                            f.write(f"{rados_name}\n")
                            cnt = cnt + 1
                            
                            # Check if end_iter > 0
                            if (data_decoded['end_iter']['cur_stripe'] > 0):
                                # Has shadow obbjects
                                for j in range(1, data_decoded['end_iter']['cur_stripe']):
                                    f.write(f"{data_decoded['end_iter']['location']['obj']['bucket']['marker']}__shadow_{data_decoded['prefix']}{j}\n")
                                    cnt = cnt + 1
                                    
                            
                            os.unlink(f"/script/tmp/{rados_name_hashed}")
                            
                logger.info(f"PG: {pgid} found {cnt} objects {global_count}")
                global_count.value += cnt
                    

    except Exception:
        print("Exception in child process:", flush=True)
        traceback.print_exc()

    return None

def build_bucket_map():
    global bucket_map
    
    batch_size = 1000
    conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
    with conn.cursor(name='streaming_cursor', cursor_factory=DictCursor) as cur:
        cur.execute('SELECT * FROM buckets')
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            for i, row in enumerate(rows):
                bucket_map[row['id']] = row['bucket_marker']
                bucket_map[row['bucket_marker']] = row['id']

def get_bucket_prefix(id):
    return bucket_map[id]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('--poolname', required=True, help='Name of the pool to use')
    parser.add_argument('--cluster', required=True, help='Name of the ceph cluster')
    parser.add_argument('--marker', required=True, help='marker ID')

    args = parser.parse_args()
    pool_name = args.poolname
    cluster = args.cluster
    

    build_bucket_map()
    pprint.pprint(bucket_map)

    # Starts forking threads

    pgids = get_pgids(args.poolname)

    with ProcessPoolExecutor(max_workers=32) as executor:
        executor.map(search_objects, pgids, repeat(cluster), repeat(pool_name), repeat(args.marker))
        
        
    print(f"Found {global_count.value} rados objects with given prefix")

