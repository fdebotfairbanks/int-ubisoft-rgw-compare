#!/usr/bin/env python3

import subprocess
import argparse
import json
import logging
import sys
import psycopg2
from concurrent.futures import ProcessPoolExecutor
import traceback


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

def clean_dups(pgid):
    logger.info(f"Cleaning for PGID {pgid}")
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        ) 
        
        cur = conn.cursor()
        cur.execute(f"""
            WITH ranked AS (
                SELECT ctid,
                    ROW_NUMBER() OVER (
                        PARTITION BY bucket_id, pg_id, object ORDER BY ctid
                    ) AS rn
                FROM objects_pg_{pgid.replace('.', '_')}
            )
            DELETE FROM objects_pg_{pgid.replace('.', '_')} o
            USING ranked r
            WHERE o.ctid = r.ctid
            AND r.rn > 1;
        """)
        conn.commit()
    except Exception:
        print("Exception in child process:", flush=True)
        traceback.print_exc()  

def clean_dup_parts_shadow():
    logger.info("Clean and sort dup arrays in shadow")

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    ) 
    
    cur = conn.cursor()
    cur.execute(f"""
UPDATE shadow
SET parts = (
    SELECT ARRAY(
        SELECT DISTINCT val
        FROM unnest(parts) AS t(val)
        ORDER BY val
    )
),
pg_ids = (
    SELECT ARRAY(
        SELECT DISTINCT val
        FROM unnest(pg_ids) AS t(val)
        ORDER BY val
    )
);    """)
    conn.commit()



if __name__ == "__main__":
    logger.info("start")

    pgids = get_pgids(pool_name)
    with ProcessPoolExecutor(max_workers=8) as ex:  # use 4 CPU cores
        ex.map(clean_dups, pgids)
        
        
    # After this some, that is not looped by pgids
    clean_dup_parts_shadow()
