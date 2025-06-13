#!/bin/bash

#Do everyting

./run.sh grab.py --db 0 --poolname zone-a.rgw.buckets.data --cluster cepha
./run.sh getstat.py --db 0 --poolname zone-a.rgw.buckets.data --cluster cepha
./run.sh grab.py --db 1 --poolname zone-b.rgw.buckets.data --cluster cephb
./run.sh getstat.py --db 1 --poolname zone-b.rgw.buckets.data --cluster cephb
