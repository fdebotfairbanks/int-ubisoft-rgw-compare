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
# parser.add_argument('--dbleftport', required=True, help='left database port')
# parser.add_argument('--dbrightport', required=True, help='right database port')


args = parser.parse_args()
db_left = args.dbleft
db_right = args.dbright
# db_left_port = args.dbleftport
# db_right_port = args.dbrightport

r_left = redis.Redis(host='localhost', port=6379, db=db_left)
r_right = redis.Redis(host='localhost', port=6379, db=db_right)

# as redis is single threaded, do not utilize threads for this script

count_left = 0
count_right = 0
unfound_left = 0
unfound_right = 0

# Which are in LEFT but not right
def compare_left():
    global count_left, count_right, unfound_left, unfound_right
    for key in r_left.scan_iter(match="object:*"):
        
        time_left = r_left.hget(key.decode('utf-8'), 'mtime')
        time_right = r_right.hget(key.decode('utf-8'), 'mtime')
        
        if not time_right:
            print(f'object {key.decode("utf-8")} exists in left, but no right')
            unfound_right = unfound_right + 1
        elif time_left.decode('utf-8') != time_right.decode('utf-8'):
            print(f'object {key.decode("utf-8")} different mtime. left: {time_left.decode("utf-8")} right: {time_right.decode("utf-8")}')
            
        count_left = count_left+1
        
        if (count_left % 10000 == 0):
            print(f'Scanned {count_left} objects on the left')
    
# Which are in RIGHT but not left
def compare_right():
    global count_left, count_right, unfound_left, unfound_right
    for key in r_right.scan_iter(match="object:*"):
        if not r_left.exists(key.decode('utf-8')):
            print(f'object {key.decode("utf-8")} exists in right, but no left')
            unfound_left = unfound_left + 1
            
        count_right = count_right+1
        if (count_right % 10000 == 0):
            print(f'Scanned {count_right} objects on the right')
    
# Create threads
t1 = threading.Thread(target=compare_left)
t2 = threading.Thread(target=compare_right)

# Start threads
t1.start()
t2.start()

# Wait for both threads to finish
t1.join()
t2.join()
    
print(f'Finished comparing {count_left} objects from left')
print(f'Finished comparing {count_right} objects from right')


