#!/bin/bash


docker run --rm --net host \
  postgres:16 \
  psql -h 127.0.0.1 -U myuser -d postgres \
  -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'mydb' AND pid <> pg_backend_pid();"


docker run --rm --net host \
  postgres:16 \
  psql -h 127.0.0.1 -U myuser -d postgres \
  -c "DROP DATABASE IF EXISTS mydb;"

docker run --rm --net host \
  postgres:16 \
  psql -h 127.0.0.1 -U myuser -d postgres \
  -c "CREATE DATABASE mydb"

docker run --rm --net host \
  postgres:16 \
  psql -h 127.0.0.1 -U myuser -d postgres \
  -c "GRANT ALL ON DATABASE mydb TO myuser"

docker exec -it redis1 redis-cli -n 2 flushdb
docker exec -it redis1 redis-cli -n 0 flushdb
