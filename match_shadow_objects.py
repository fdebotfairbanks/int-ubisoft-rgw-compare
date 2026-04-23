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
import logging
import time
from concurrent.futures import ProcessPoolExecutor
import subprocess, redis
import traceback
import psycopg2
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import signal, sys
from datetime import datetime
import multiprocessing
from psycopg2.extras import DictCursor
import calendar
import tempfile
import os

parser = argparse.ArgumentParser(description="")
parser.add_argument('--poolname', required=True, help='Name of the pool to use')
parser.add_argument('--cluster', required=True, help='Name of the ceph cluster')

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


# Bucketmapping
bucket_map = {}

# Configuratie
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "mydb"
DB_USER = "myuser"
DB_PASS = "mypassword"

LOG_LEVEL = logging.DEBUG


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


def get_bucket_prefix(bucket_id, conn):
    if bucket_id in bucket_map:
        return bucket_map[bucket_id]
    
    
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT * FROM buckets WHERE id = %s", (bucket_id,))
        row = cur.fetchone()

        bucket_map[row['id']] = row['bucket_marker']
        logger.debug(f"Returning marker {row['bucket_marker']}")
        return row['bucket_marker']

    return  None 

def get_pgids(pool_name):
    """Get PG IDs for a given pool."""
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

def get_manifest(ioctx, object_name: str):
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
        os.unlink(tmpfile_path)
        return result
    except:
        return None

def shadow2_to_redis(pgid):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        ) 
        
        pool = redis.ConnectionPool(host="localhost", port=6379, db=0, max_connections=128)
        r = redis.Redis(connection_pool=pool)
        redis_pipe = r.pipeline(transaction=False)
        
        # Batchgrootte
        BATCH_SIZE = 1000
        count = 0
        with conn.cursor(name='streaming_cursor', cursor_factory=DictCursor) as cur:
            cur.itersize = BATCH_SIZE
            # cur.execute("SELECT bucket_id,pg_id,object FROM objects WHERE pg_id = %s AND mtime IS NULL", (pgid,))
            cur.execute("SELECT object,id FROM shadow2 WHERE pg_id = %s", (pgid,))

            batch_num = 0
            while True:
                rows = cur.fetchmany(BATCH_SIZE)
                
                if not rows:
                    break
                for i, row in enumerate(rows):
                    redis_pipe.set(row['object'], json.dumps({'pg_id': pgid, 'id': row['id']}) )
                    count = count+1
                    if count % 1000 == 0:
                        redis_pipe.execute()
                        
            redis_pipe.execute()
                
    except Exception:
        print("Exception in child process:", flush=True)
        traceback.print_exc()

    return None
   
    
    


def match_objects(pgid):
    multiprocessing.current_process().name = f"worker pgid: {pgid}"
    try:
        logger.info(f"Inspecting objects in pg {pgid}")
        
        # Ceph
        cluster = rados.Rados(conffile=f'/etc/ceph/{cluster_name}.conf')
        cluster.connect()        
        ioctx = cluster.open_ioctx(pool_name)

        pool = redis.ConnectionPool(host="localhost", port=6379, db=0, max_connections=128)
        r = redis.Redis(connection_pool=pool)
        

        # Connectie openen
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        ) 
        update_conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        ) 

        # Cursor aanmaken
        cur = conn.cursor()
        update_cur = update_conn.cursor()


        # Batchgrootte
        BATCH_SIZE = 1000
        count = 0
        with conn.cursor(name='streaming_cursor', cursor_factory=DictCursor) as cur:
            cur.itersize = BATCH_SIZE
            cur.execute("SELECT bucket_id,pg_id,object FROM objects WHERE pg_id = %s AND mtime IS NULL", (pgid,))
            # cur.execute("SELECT bucket_id,pg_id,object,id FROM objects WHERE pg_id = %s --AND mtime IS NULL", (pgid,))

            batch_num = 0
            while True:
                rows = cur.fetchmany(BATCH_SIZE)
                if not rows:
                    break
                for i, row in enumerate(rows):
                    # Process rows here
                    logger.info(f"Processing row {i} of batch {batch_num} for pgid={pgid} => {row['object']} on bucket_id {row['bucket_id']}")
                    
                    bucket_prefix = get_bucket_prefix(row['bucket_id'], conn)
                    rados_object_name = f"{bucket_prefix}_{row['object']}"
                    
                    try:
                        size, mtime = ioctx.stat(rados_object_name)
                        mtime_timestamp = str(calendar.timegm(mtime))
                        logger.info(f"  - mtime: {mtime_timestamp}")
                        
                        update_cur.execute("UPDATE objects SET mtime=to_timestamp(%s), size=%s WHERE bucket_id=%s AND pg_id=%s AND object=%s", 
                                    (mtime_timestamp, size, row['bucket_id'], row['pg_id'], row['object'], )
                                    )
                        # Get manifest
                        
                        manifest = get_manifest(ioctx, rados_object_name)
                        stripped_prefix = manifest['prefix'].removesuffix("_")
                        
                        # Lookup prefix in redis
                        d = r.get(stripped_prefix)
                        if d:
                            val = json.loads(d)
                            update_cur.execute("UPDATE shadow2 SET object_id=%s, object_pg_id=%s WHERE pg_id=%s AND id=%s", (row['id'], row['pg_id'], val['pg_id'], val['id']))
                    except rados.ObjectNotFound:
                        update_cur.execute("UPDATE objects SET not_found=true WHERE bucket_id=%s AND pg_id=%s AND object=%s",(row['bucket_id'], row['pg_id'], row['object'],)) 
                            
                    
                    count += 1
                    
                    if count % 100:
                        update_conn.commit()

                    # get_mtime(rados_object_name, conn, cluster, redis_pipe, row['bucket_id'])
            
        # Final commit
        logger.info("final commit")
        update_conn.commit()
                    
    except Exception:
        print("Exception in child process:", flush=True)
        traceback.print_exc()

    return None
        

def get_mtimes(pgid):
    logger.info("get_mtimes()")
    try:
        # Ceph
        cluster = rados.Rados(conffile=f'/etc/ceph/{cluster_name}.conf')
        cluster.connect()        
        ioctx = cluster.open_ioctx(pool_name)

        
        # Get mtimes of unreferenced shadow2 objects
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        ) 
        update_conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        ) 
        
        cur = conn.cursor()
        update_cur = update_conn.cursor()
        
        # Batchgrootte
        BATCH_SIZE = 1000
        count = 0
        
        with conn.cursor(name='streaming_cursor', cursor_factory=DictCursor) as cur:
            cur.itersize = BATCH_SIZE
            # cur.execute("SELECT bucket_id,pg_id,object FROM objects WHERE pg_id = %s AND mtime IS NULL", (pgid,))
            cur.execute("SELECT id, object, bucket_id FROM shadow2 WHERE pg_id = %s AND mtime IS NULL AND object_id IS NULL", (pgid,))

            batch_num = 0
            while True:
                rows = cur.fetchmany(BATCH_SIZE)
                
                if not rows:
                    break
                for i, row in enumerate(rows):

                    bucket_prefix = get_bucket_prefix(row['bucket_id'], conn)
                    rados_object_name = f"{bucket_prefix}__shadow_{row['object']}_1"
                    
                    try:
                        logger.info('test1')
                        size, mtime = ioctx.stat(rados_object_name)
                        mtime_timestamp = str(calendar.timegm(mtime))
                        update_cur.execute('UPDATE shadow2 SET mtime=to_timestamp(%s) WHERE pg_id = %s AND id =%s', (mtime_timestamp, pgid, row['id'],))
                    except rados.ObjectNotFound:
                        logger.info('test2')
                        update_cur.execute('UPDATE shadow2 SET not_found=true WHERE pg_id = %s AND id =%s', (pgid, row['id'],))
                    if count % 100:
                        update_conn.commit()

            update_conn.commit()

                      
        
    except Exception:
        print("Exception in child process:", flush=True)
        traceback.print_exc()

    return None

def migrate_db():
    logger.info('migrate_db()')
    # DB changes
    conn_global = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    ) 

    cur_global = conn_global.cursor()
    
    cur_global.execute("ALTER TABLE objects ADD COLUMN IF NOT EXISTS not_found BOOLEAN")
    cur_global.execute("ALTER TABLE shadow2 ADD COLUMN IF NOT EXISTS not_found BOOLEAN")
    
    conn_global.commit()
    conn_global.close()
                       
    logger.info('migrate_db() done')



pgids = get_pgids(pool_name)

migrate_db()

with ProcessPoolExecutor(max_workers=16) as executor:
    executor.map(shadow2_to_redis, pgids)

with ProcessPoolExecutor(max_workers=16) as executor:
    executor.map(match_objects, pgids)

with ProcessPoolExecutor(max_workers=16) as executor:
    executor.map(get_mtimes, pgids)
