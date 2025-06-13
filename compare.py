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

parser = argparse.ArgumentParser(description="Script that requires --db-left --db-right.")
parser.add_argument('--dbleft', required=True, help='left database')
parser.add_argument('--dbright', required=True, help='right database')


args = parser.parse_args()
db_left = args.dbleft
db_right = args.dbright

r_left = redis.Redis(host='localhost', port=6379, db=db_left)
r_right = redis.Redis(host='localhost', port=6379, db=db_right)

# as redis is single threaded, do not utilize threads for this script

count_left=0
count_right=0
unfoudn_left=0
unfound_right=0

# Which are in LEFT but not right
for key in r_left.scan_iter(match="object:*"):
    
    time_left = r_left.hget(key.decode('utf-8'), 'mtime')
    time_right = r_right.hget(key.decode('utf-8'), 'mtime')
    
    if not time_right:
        print(f'object {key.decode("utf-8")} exists in left, but no right')
        unfound_right = unfound_right + 1
    elif time_left.decode('utf-8') != time_right.decode('utf-8'):
        print(f'object {key.decode("utf-8")} different mtime. left: {time_left.decode("utf-8")} right: {time_right.decode("utf-8")}')
        
    count_left = count_left+1
    
# Which are in RIGHT but not left

for key in r_right.scan_iter(match="object:*"):
    if not r_left.exists(key.decode('utf-8')):
        print(f'object {key.decode("utf-8")} exists in right, but no left')
        unfound_left = unfound_left + 1
        
    count_right = count_right+1
    
    
print(f'Finished comparing {count_left} objects from left')
print(f'Finished comparing {count_right} objects from right')