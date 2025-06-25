#!/bin/bash

#Do everyting

./run.sh grab.py --db 0 --dbport 6379 --poolname zone-a.rgw.buckets.data --cluster cepha
./run.sh getstat.py --db 0 --dbport 6379 --poolname zone-a.rgw.buckets.data --cluster cepha

./run.sh grab.py --db 0 --dbport 6380 --poolname zone-b.rgw.buckets.data --cluster cephb
./run.sh getstat.py --db 0 --dbport 6380 --poolname zone-b.rgw.buckets.data --cluster cephb
