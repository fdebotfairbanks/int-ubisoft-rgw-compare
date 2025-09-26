#!/usr/bin/env python3

import subprocess
import argparse
import json
import logging
import sys
import psycopg2


# Configuratie
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "mydb"
DB_USER = "myuser"
DB_PASS = "mypassword"

# Configure logger
logger = logging.getLogger("myapp")
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] pid=%(process)d %(message)s",  # format with timestamp
    datefmt="%Y-%m-%d %H:%M:%S"                # timestamp format
)
handler.setFormatter(formatter)

logger.addHandler(handler)

logger.debug("LOG!")



parser = argparse.ArgumentParser(description="")
parser.add_argument('--poolname', required=True, help='Name of the pool to use')
parser.add_argument('--cluster', required=True, help='Name of the ceph cluster')

args = parser.parse_args()
pool_name = args.poolname
cluster = args.cluster


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

if __name__ == "__main__":
    logger.info("ok?")
    
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    ) 
    cur = conn.cursor()

    with open("/script/sql", "r") as f:
        sql = f.read()
        # Queries uitvoeren
        cur.execute(sql)

        # Resultaten committen (bij updates/inserts)
        conn.commit()

    BATCH_SIZE = 50;
    count = 0;
    
    for pgid in get_pgids(pool_name):
        count += 1
        logger.info(f"PGID: {pgid}")
        
       
        cur.execute(f"CREATE TABLE IF NOT EXISTS objects_pg_{pgid.replace('.', '_')} PARTITION OF objects FOR VALUES IN ('{pgid}');")
        # cur.execute(f"CREATE TABLE IF NOT EXISTS multipart_pg_{pgid.replace('.', '_')} PARTITION OF multipart FOR VALUES IN ('{pgid}');")
        # cur.execute(f"CREATE TABLE IF NOT EXISTS shadow_pg_{pgid.replace('.', '_')} PARTITION OF shadow FOR VALUES IN ('{pgid}');")
        if count % BATCH_SIZE == 0:
            conn.commit()
        
        
    for i in range(64):
        count += 1
        cur.execute(f"CREATE TABLE IF NOT EXISTS shadow_part_{i} PARTITION OF shadow FOR VALUES WITH (MODULUS 64, REMAINDER {i})")
        cur.execute(f"CREATE TABLE IF NOT EXISTS multipart_part_{i} PARTITION OF multipart FOR VALUES WITH (MODULUS 64, REMAINDER {i})")
        if count % BATCH_SIZE == 0:
            conn.commit()

    conn.commit()