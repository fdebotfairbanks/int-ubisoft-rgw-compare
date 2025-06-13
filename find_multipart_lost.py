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

parser = argparse.ArgumentParser(description="Script that requires --db and --poolname.")
parser.add_argument('--db', required=True, help='Path to the database or connection string')

args = parser.parse_args()
redis_db = args.db

multipart_pattern = re.compile(
    r'^(?P<full_id>[a-f0-9\-]+\.\d+\.\d+)__multipart_(?P<objectname>.+)\.(?P<postfix>\d+~[^.]+)$'
)

r = redis.Redis(host='localhost', port=6379, db=redis_db)

for key in r.scan_iter(match="multipart:*"):
    object_name = re.sub(r"^multipart:", "", key.decode('utf-8'))
    print(f'Checking {object_name}')

    multipart_match = multipart_pattern.match(object_name)
    if multipart_match:    

        exists =  r.exists(f'object:{multipart_match.groupdict()["full_id"]}_{multipart_match.groupdict()["objectname"]}')
        if not exists:
            print(f' - found lost')
            
    else:
        print('Could not match?')
    
