#!/bin/env python3

import subprocess
import json
import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import pprint
import re
import rados
from typing import Dict
import tempfile

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

parser = argparse.ArgumentParser(description="")
parser.add_argument('--poolname', required=True, help='Name of the pool to use')
parser.add_argument('--cluster', required=True, help='Name of the ceph cluster')
parser.add_argument('--marker', required=True, help='Bucket marker to inspect')

args = parser.parse_args()
pool_name = args.poolname
cluster_name = args.cluster
marker = args.marker


multipart_pattern = re.compile(
    r'^(?P<full_id>[a-f0-9\-]+\.\d+\.\d+)__multipart_(?P<objectname>.+)\.(?P<postfix>\d+~[^.]+)\.(?P<number>\d+)$'
)

multipart_legacy_pattern = re.compile(
    r'(?P<full_id>[a-f0-9\-]+\.\d+\.\d+)__multipart_(?P<objectname>.+)\.(?P<postfix>[^.]+)\.(?P<number>\d+)$'
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

# Connect to the cluster
cluster = rados.Rados(conffile=f'/etc/ceph/{cluster_name}.conf')
cluster.connect()
ioctx = cluster.open_ioctx(pool_name)


def get_pgids(cluster_name, pool_name):
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

        return pgids
    except subprocess.CalledProcessError as e:
        logger.error(f"[ERROR] Failed to get PGs: {e.stderr}")
        return []


def run_rgw_admin(cmd):
    """Run radosgw-admin and return parsed JSON output."""
    try:
        result = subprocess.run(
            ['radosgw-admin', '--cluster', cluster_name] + cmd,
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

    return bucket_id_map


def list_objects_in_pg(pool_name, pgid):
    """Run 'rados -p pool --pgid pgid ls' and return list of objects."""
    try:
        result = subprocess.run(
            ["rados", "--cluster", cluster_name, "-p", pool_name, "--pgid", pgid, "ls"],
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
        return result

    except subprocess.CalledProcessError as e:
        logger.info("Decode failed with code:", e.returncode)
        logger.info("STDERR:", e.stderr)
        logger.info("STDOUT:", e.stdout)

    except Exception as e:
        logger.info(f'bah: {e}')


bucketmap = build_bucket_id_map()
pprint.pprint(bucketmap)
if marker in bucketmap:
    logger.info(f'marker {marker} belongs to bucket {bucketmap[marker]} it is an existing  bucket')
    # exit(1)


for pgid in get_pgids(cluster_name, pool_name):
    objects = list_objects_in_pg(pool_name, pgid)

    for object in objects:
        multipart_match = multipart_pattern.match(object)
        multipart_legacy_match = multipart_legacy_pattern.match(object)
        shadow_match = shadow_pattern.match(object)
        regular_match = regular_pattern.match(object)

        if multipart_match:
            continue
        elif multipart_legacy_match:
            continue
        elif shadow_match:
            continue
        if regular_match:
            try:
                full_id = regular_match.groupdict()["full_id"]
                # logger.info(f"Checking {full_id} == {marker} ?")
                if regular_match.groupdict()["full_id"] == marker:
                    manifest = get_manifest(object)
                    bucket_name = manifest['begin_iter']['location']['obj']['bucket']['name']
                    logger.info(f'Bucket name {bucket_name}')
                    exit(1)
            except Exception:
                logger.debug(f"Skipping {object}")
