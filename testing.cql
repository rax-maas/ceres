CREATE KEYSPACE ceres
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

SELECT *
FROM series_sets;

SELECT metric_name
FROM metric_names
WHERE tenant = 't-1';

SELECT tag_key
FROM tag_keys
WHERE tenant = 't-1'
  AND metric_name = 'cpu_idle';

SELECT tag_value
FROM tag_values
WHERE tenant = 't-1'
  AND metric_name = 'cpu_idle'
  AND tag_key = 'os';

SELECT series_set
FROM series_sets
WHERE tenant = 't-1'
  AND metric_name = 'cpu_idle'
  AND tag_key = 'os'
  AND tag_value = 'linux';

SELECT *
FROM data_raw;

SELECT *
FROM data_raw
WHERE tenant = 't-1'
  AND series_set = 'cpu_idle,deployment=dev,host=h-3,os=linux';


// From generated DDL

create table data_raw
(
    tenant text,
    series_set text,
    ts timestamp,
    value double,
    primary key ((tenant, series_set), ts)
)
    with caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
     and compaction = {'max_threshold': '32', 'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
     and compression = {'class': 'org.apache.cassandra.io.compress.LZ4Compressor', 'chunk_length_in_kb': '64'}
     and dclocal_read_repair_chance = 0.1;

create table metric_names
(
    tenant text,
    metric_name text,
    primary key (tenant, metric_name)
)
    with caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
     and compaction = {'max_threshold': '32', 'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
     and compression = {'class': 'org.apache.cassandra.io.compress.LZ4Compressor', 'chunk_length_in_kb': '64'}
     and dclocal_read_repair_chance = 0.1;

create table series_sets
(
    tenant text,
    metric_name text,
    tag_key text,
    tag_value text,
    series_set text,
    primary key ((tenant, metric_name), tag_key, tag_value, series_set)
)
    with caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
     and compaction = {'max_threshold': '32', 'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
     and compression = {'class': 'org.apache.cassandra.io.compress.LZ4Compressor', 'chunk_length_in_kb': '64'}
     and dclocal_read_repair_chance = 0.1;

create table tag_keys
(
    tenant text,
    metric_name text,
    tag_key text,
    primary key ((tenant, metric_name), tag_key)
)
    with caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
     and compaction = {'max_threshold': '32', 'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
     and compression = {'class': 'org.apache.cassandra.io.compress.LZ4Compressor', 'chunk_length_in_kb': '64'}
     and dclocal_read_repair_chance = 0.1;

create table tag_values
(
    tenant text,
    metric_name text,
    tag_key text,
    tag_value text,
    primary key ((tenant, metric_name), tag_key, tag_value)
)
    with caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
     and compaction = {'max_threshold': '32', 'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
     and compression = {'class': 'org.apache.cassandra.io.compress.LZ4Compressor', 'chunk_length_in_kb': '64'}
     and dclocal_read_repair_chance = 0.1;

