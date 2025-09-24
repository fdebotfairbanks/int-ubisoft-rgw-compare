CREATE TABLE IF NOT EXISTS buckets (
    id SERIAL PRIMARY KEY,
    name TEXT,
    bucket_marker VARCHAR(128) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS objects (
    bucket_id BIGINT NOT NULL REFERENCES buckets(id),
    pg_id CHAR(8) NOT NULL,
    object TEXT NOT NULL,
    found_in_index BOOLEAN,
    mtime TIMESTAMP,
    has_non_print BOOLEAN
) PARTITION BY LIST (pg_id);

CREATE TABLE IF NOT EXISTS multipart (
    bucket_id BIGINT NOT NULL REFERENCES buckets(id),
    object TEXT NOT NULL,
    postfix TEXT NOT NULL,
    has_non_print BOOLEAN,
    parts CHAR(8)[],
    pg_ids CHAR(8)[],
    UNIQUE (bucket_id, object)
) PARTITION BY HASH (bucket_id);

CREATE TABLE IF NOT EXISTS shadow (
    bucket_id BIGINT NOT NULL REFERENCES buckets(id),
    object TEXT NOT NULL,
    has_non_print BOOLEAN,
    parts CHAR(8)[],
    pg_ids CHAR(8)[],
    UNIQUE (bucket_id, object)
) PARTITION BY HASH (bucket_id);

CREATE TABLE IF NOT EXISTS other (
    id SERIAL PRIMARY KEY,
    pg_id CHAR(8) NOT NULL,
    object TEXT NOT NULL
)