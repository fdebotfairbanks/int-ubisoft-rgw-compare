#!/bin/bash

PORT=5432

# Batch drop
tables=$(docker run --rm --net host postgres:16 psql -h 127.0.0.1 -p $PORT -U myuser -d mydb  -Atc "SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent = 'objects'::regclass")
shadow2=$(docker run --rm --net host postgres:16 psql -h 127.0.0.1 -p $PORT -U myuser -d mydb  -Atc "SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent = 'multipart'::regclass")


# Batch drop
echo "$tables" | xargs -n 250 | while read batch; do
    # Maak 1 DROP TABLE statement
    sql="DROP TABLE $(echo $batch | tr ' ' ',');"
    echo $sql
    docker run --rm --net host postgres:16 psql -h 127.0.0.1 -p $PORT -U myuser -d mydb  -Atc  "$sql"
done

echo "$shadow2" | xargs -n 250 | while read batch; do
    # Maak 1 DROP TABLE statement
    sql="DROP TABLE $(echo $batch | tr ' ' ',');"
    echo $sql
    docker run --rm --net host postgres:16 psql -h 127.0.0.1 -p $PORT  -U myuser -d mydb  -Atc  "$sql"
done


docker run --rm --net host postgres:16 psql -h 127.0.0.1 -p $PORT -U myuser -d mydb  -Atc "DROP TABLE objects CASCADE";
docker run --rm --net host postgres:16 psql -h 127.0.0.1 -p $PORT -U myuser -d mydb  -Atc "DROP TABLE multipart CASCADE";
docker run --rm --net host postgres:16 psql -h 127.0.0.1 -p $PORT -U myuser -d mydb  -Atc "DROP TABLE shadow CASCADE";
docker run --rm --net host postgres:16 psql -h 127.0.0.1 -p $PORT -U myuser -d mydb  -Atc "DROP TABLE shadow2 CASCADE";
docker run --rm --net host postgres:16 psql -h 127.0.0.1 -p $PORT -U myuser -d mydb  -Atc "DROP TABLE buckets CASCADE";
docker run --rm --net host postgres:16 psql -h 127.0.0.1 -p $PORT -U myuser -d mydb  -Atc "DROP TABLE other CASCADE";

docker exec -it redis1 redis-cli -n 2 flushdb
docker exec -it redis1 redis-cli -n 0 flushdb
