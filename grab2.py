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

# Configuratie
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "mydb"
DB_USER = "myuser"
DB_PASS = "mypassword"

LOG_LEVEL = logging.DEBUG

parser = argparse.ArgumentParser(description="")
parser.add_argument('--poolname', required=True, help='Name of the pool to use')
parser.add_argument('--cluster', required=True, help='Name of the ceph cluster')

args = parser.parse_args()
pool_name = args.poolname
cluster = args.cluster

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

# r = redis.Redis(host='localhost', port=redis_port, db=redis_db)
pool = redis.ConnectionPool(host="localhost", port=6379, db=0, max_connections=128)
r = redis.Redis(connection_pool=pool)

r_progress = redis.Redis(host='localhost', port=6379, db=2)
# r_progress.flushdb() # remove later

# Configure logger
logger = logging.getLogger("myapp")
logger.setLevel(LOG_LEVEL)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(LOG_LEVEL)
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] pid=%(process)d %(message)s",  # format with timestamp
    datefmt="%Y-%m-%d %H:%M:%S"                # timestamp format
)
handler.setFormatter(formatter)

logger.addHandler(handler)

logger.debug("LOG!")

# DB connection global

conn_global = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS
) 

cur_global = conn_global.cursor()


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
        logger.error(f"Command failed: {' '.join(e.cmd)}")
        logger.error(f"Error: {e.stderr}")
        return None

def get_bucket_stats(bucket):
    """Fetch stats for a single bucket and return mapping entries."""
    logger.info(f"Get ID for bucket {bucket}")
    stats = run_rgw_admin(['bucket', 'stats', '--bucket', bucket])
    if stats and 'id' in stats:
        bucket_id_map = {
            stats['id']: bucket
        }
        # Sommige versies hebben ook een 'marker'
        if 'marker' in stats:
            bucket_id_map[stats['marker']] = bucket
        return bucket_id_map
    return {}

def build_bucket_id_map():
    max_workers = 32
    bucket_id_map = {}

    # Step 1: Get list of all buckets
    bucket_names = run_rgw_admin(['bucket', 'list'])
    if bucket_names is None:
        return {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_bucket = {executor.submit(get_bucket_stats, bucket): bucket for bucket in bucket_names}
        for future in as_completed(future_to_bucket):
            try:
                result = future.result()
                bucket_id_map.update(result)
            except Exception as e:
                logger.error(f"Failed to process bucket {future_to_bucket[future]}: {e}")


    # # Step 2: For each bucket, get the bucket ID
    # for bucket in bucket_names:
    #     logger.info(f"Getting ID for bucket {bucket}")
    #     stats = run_rgw_admin(['bucket', 'stats', '--bucket', bucket])
    #     if stats and 'id' in stats:
            
    #         bucket_id = stats['id']
    #         bucket_id_map[bucket_id] = bucket
            
    #         bucket_id_map[stats['marker']] = bucket

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
        
        for pg in pg_data["pg_stats"]:
            pg_stats[pg["pgid"]] = {"objects": pg["stat_sum"]["num_objects"]}
            stats['total']['objects'] = stats['total']['objects'] + pg["stat_sum"]["num_objects"]
            
        stats['total']['pg_num'] = len(pgids)
        
        
        return pgids
    except subprocess.CalledProcessError as e:
        logger.error(f"[ERROR] Failed to get PGs: {e.stderr}")
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
        logger.error(f"[ERROR] PG {pgid}: {e.stderr}")
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
            logger.info(f'Getting objects from {pgid}')
            grab_objects(pgid)
            
        finally:
            q.task_done()

bucket_markers = {}

def get_id_by_bucket_marker(bucket_marker, conn):
    if bucket_marker in bucket_markers:
        return bucket_markers[bucket_marker]

    with conn.cursor() as cur:
        # probeer te selecteren
        # logger.debug(f"query: SELECT id FROM buckets WHERE bucket_marker = {bucket_marker}")
        cur.execute("SELECT id FROM buckets WHERE bucket_marker = %s", (bucket_marker, ))
        row = cur.fetchone()
        if row is not None:
            bucket_id = row[0]
        else:
            # insert als niet bestaat
            cur.execute(
                "INSERT INTO buckets (bucket_marker) VALUES (%s) RETURNING id",
                (bucket_marker,)
            )
            bucket_id = cur.fetchone()[0]
            conn.commit()  # commit na insert

    # cache bijwerken
    bucket_markers[bucket_marker] = bucket_id
    return bucket_markers[bucket_marker]
    

def has_nonprintable(b: bytes) -> bool:
    """
    Return True if the line contains non-printable bytes,
    ignoring TAB (\t), LF (\n), and CR (\r).
    """
    if isinstance(b, str):
        b = b.encode("utf-8", errors="ignore")
        
    for c in b:
        if c in (9, 10, 13):  # tab, newline, carriage return
            continue
        if 32 <= c <= 126:    # printable ASCII
            continue
        return True


def grab_objects(pgid):
    try:
        logger.debug(f"grab_objects({pgid})")
        start_time = time.time()
        count=0
        objects = list_objects_in_pg(pool_name, pgid)
        
        # pprint.pprint(objects)
        
        # Open database
        pipe = r.pipeline(transaction=False)
        
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

        # Batchgrootte
        BATCH_SIZE = 1000
        
        for object in objects:
            has_non_print = False
            logger.debug(f"Inspecting {object}")
            if has_nonprintable(object):
                object = object.encode('unicode_escape').decode('ascii')
                has_non_print = True
                
            logger.debug(f"Inspecting {object}")
            count = count+1
            
            if count % BATCH_SIZE == 0:
                r_progress.set(f"progress:{pgid}", count)
                
            # print(f"Checking {object}")
            multipart_match = multipart_pattern.match(object)
            if multipart_match:
                logger.debug(f"Found multipart {object}")
                bucket_id = get_id_by_bucket_marker(multipart_match.groupdict()["full_id"], conn)
                key = f'multipart:{multipart_match.groupdict()["full_id"]}__multipart_{multipart_match.groupdict()["objectname"]}.{multipart_match.groupdict()["postfix"]}'
                
                cur.execute("INSERT INTO multipart (bucket_id, object, postfix, has_non_print) VALUES(%s, %s, %s, %s) ON CONFLICT (bucket_id, object) DO NOTHING;", 
                            (bucket_id, 
                             multipart_match.groupdict()["objectname"],
                             multipart_match.groupdict()["postfix"],
                             has_non_print
                             )
                            )
                conn.commit()

                pipe.lpush("queue:multipart", json.dumps({'bucket_id': bucket_id, 
                                                        'object': multipart_match.groupdict()["objectname"], 
                                                        'postfix': multipart_match.groupdict()["postfix"],
                                                        'number': multipart_match.groupdict()["number"],
                                                        'pg_id': pgid
                                                        }))

                # pipe.sadd(key, multipart_match.groupdict()['number'])
                # stuur batch naar Redis
                if count % BATCH_SIZE == 0:
                    pipe.execute()
                    conn.commit()
                
                continue

            shadow_match = shadow_pattern.match(object)
            if shadow_match:
                bucket_id = get_id_by_bucket_marker(shadow_match.groupdict()["full_id"], conn)
                logger.debug(f"Found shadow {object}")
                key = f'shadow:{shadow_match.groupdict()["full_id"]}__shadow_{shadow_match.groupdict()["rest"]}'
                cur.execute("INSERT INTO shadow (bucket_id, object, has_non_print) VALUES(%s, %s, %s) ON CONFLICT (bucket_id, object) DO NOTHING;", (bucket_id, shadow_match.groupdict()["rest"], has_non_print))
                conn.commit()
                pipe.lpush("queue:shadow", json.dumps({'bucket_id': bucket_id, 
                                                        'object': shadow_match.groupdict()["rest"], 
                                                        'part': shadow_match.groupdict()['suffix'],
                                                        'pg_id': pgid
                                                        }))
#                cur.execute("UPDATE shadow SET parts = array_append(parts, %s) WHERE bucket_id = %s AND object = %s", (shadow_match.groupdict()['suffix'], bucket_id, shadow_match.groupdict()["rest"]) )
                
# UPDATE object_groups
# SET suffixes = array_append(suffixes, 4)
# WHERE basename = 'object';

                # pipe.sadd(key, shadow_match.groupdict()['suffix'])
                # stuur batch naar Redis
                if count % BATCH_SIZE == 0:
                    pipe.execute()
                    conn.commit()
                
                continue
            
            regular_match = regular_pattern.match(object)
            if regular_match:
                logger.debug(f"Found regular {object}")

                key = f'object:{regular_match.groupdict()["full_id"]}_{regular_match.groupdict()["objectname"]}'
                
                bucket_id = get_id_by_bucket_marker(regular_match.groupdict()["full_id"], conn)
                
                logger.debug(f"Found bucket_id: {bucket_id}")
                
                cur.execute("INSERT INTO objects (bucket_id, pg_id, object, has_non_print) VALUES(%s, %s, %s, %s)", (bucket_id, pgid, regular_match.groupdict()["objectname"], has_non_print))
                    
                continue

            logger.error(f"I should not  reach here, but I'll store the name ({object})")
            
            cur.execute("INSERT INTO other (pg_id, object) VALUES(%s, %s)", (pgid, object))
            
            
        # eventuele restjes
        if count % BATCH_SIZE != 0:
            logger.info("Flushing commit")
            conn.commit()
            pipe.execute()
        # Record end time
        end_time = time.time()
        # Calculate elapsed time in seconds
        elapsed = end_time - start_time

        logger.info(f"Stored {len(objects)} for pg {pgid} in {elapsed:.1f}s ")
        r_progress.set(f"finished:{pgid}", len(objects))
        r_progress.set(f"progress:{pgid}", count)
    except Exception:
        print("Exception in child process:", flush=True)
        traceback.print_exc()  

def print_stats():
    for key in r_progress.scan_iter(match="finished:*", count=1000):
        stats['done']['pg_num'] += 1

    for key in r_progress.scan_iter(match="progress:*", count=1000):
        stats['done']['objects'] += int(r_progress.get(key))

    #stats['script_start'] = time.time()

    per_sec = 0
    if stats['done']['objects'] > 0:
        per_sec = stats['done']['objects'] / ( time.time() - stats['script_start'] )
        

    logger.info(f"STATS: PG's {stats['done']['pg_num']}/{stats['total']['pg_num']} - objects: {stats['done']['objects']}/{stats['total']['objects']} ({per_sec:.1f} /sec)")
    
    # Reset is for next loop
    stats['done']['pg_num'] = 0
    stats['done']['objects'] = 0
    stats['previous']['objects'] = stats['done']['objects']


def stats_printer(stop_event):
    """Background thread that prints stats every 5s until stop_event is set."""
    while not stop_event.is_set():
        try:
            print_stats()
        except Exception as e:
            logger.error(f"[stats] Error: {e}")
        stop_event.wait(5)   # sleep 5s, but wake early if stop_event is set

def update_worker(stop_event):
    logger.info("update_worker() running")
    
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

    
    while not stop_event.is_set():
        try:
            logger.info(f"update_worker() doing stuff")
            # Shadow
            while True:
                msg = r.rpop("queue:shadow")  # 1 = max aantal keys
                
                if msg is not None:
                    logger.debug(f"Ontvangen: {msg}")
                    data = json.loads(msg.decode('utf-8'))
                    
                    
                    cur.execute("UPDATE shadow SET parts = array_append(parts, %s), pg_ids = array_append(pg_ids, %s) WHERE bucket_id = %s AND object = %s", 
                                (data['part'], 
                                 data['pg_id'],
                                 data['bucket_id'], 
                                 data['object']) 
                                )
                else:
                    logger.info("Geen messages (meer)")
                    break
                
            # Multipart
            while True:
                msg = r.rpop("queue:multipart")  # 1 = max aantal keys
                
                if msg is not None:
                    logger.debug(f"Ontvangen: {msg}")
                    data = json.loads(msg.decode('utf-8'))
                    
                    
                    cur.execute("UPDATE multipart SET parts = array_append(parts, %s), pg_ids = array_append(pg_ids, %s) WHERE bucket_id = %s AND object = %s", 
                                (data['number'],
                                 data['pg_id'],
                                 data['bucket_id'], 
                                 data['object']) 
                                )
                else:
                    logger.info("Geen messages (meer)")
                    break
            
                
            conn.commit()
                
        except Exception as e:
            logger.error(f"[stats] Error: {e}")
        stop_event.wait(1)   # sleep 5s, but wake early if stop_event is set

bucket_map = build_bucket_id_map()
logger.info("Bucket ID to Name Mapping:")
for bucket_id, name in bucket_map.items():
    cur_global.execute("INSERT INTO buckets (name, bucket_marker) VALUES (%s, %s) ON CONFLICT (bucket_marker) DO NOTHING", (name, bucket_id))
    logger.info(f"{bucket_id} : {name}")
    
conn_global.commit()

logger.info("\n\n")


pgids = get_pgids(pool_name)

# Compare pg_stats[pg_id]['objects] in redis to see if it needs scanning

for key in r_progress.scan_iter(match="finished:*", count=1000):
    pg_already_scanned = key.decode('utf-8').split(":")[1]
    
    objects_scanned = int(r_progress.get(f'progress:{pg_already_scanned}').decode('utf-8'))
    objects_in_pg = pg_stats[pg_already_scanned]['objects']
    
    logger.info(f"Already scanned {pg_already_scanned} with {objects_scanned} pg_stats shows {objects_in_pg}")
    if abs(objects_scanned - objects_in_pg) > 25:
        logger.info(f" - pg maybe not completely scanned, rescanning it")
    else:
        pgids.remove(pg_already_scanned)




stats['script_start'] = time.time()

# Start stats thread
stop_event = threading.Event()
stats_thread = threading.Thread(target=stats_printer, args=(stop_event,))
stats_thread.start()

update_thread = threading.Thread(target=update_worker, args=(stop_event,))
update_thread.start()

with ProcessPoolExecutor(max_workers=32) as executor:
    executor.map(grab_objects, pgids)
    
        
# Signal stats thread to stop
stop_event.set()
stats_thread.join()
update_thread.join()



print_stats()
logger.info("done")
