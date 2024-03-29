## Design

### System Diagram

The following diagram summarizes the internal and external aspects of the Ceres application. The three color bands highlight the major functions of Ceres. A single instance of Ceres can be deployed to fulfill all three functions or any combination of replicas can be deployed to selectively scale to and serve each function.

![](docs/ceres-internal-system-overview.drawio.png)

### Tables

The Cassandra tables in the `ceres` keyspace (by default) are organized into two categories:
- **metadata** : tables that enable metadata lookup directly or to enable queries
- **data** : tables that store the timestamped values

#### Metadata

> Columns are designated as partitioning key (PK) of primary key, clustering key (CK) of primary key, or blank for non-key columns.

`metric_names`:

&nbsp; | Name | Notes
---|---|---
PK | tenant |
CK | metric_name | 

`series_set_hashes`

&nbsp; | Name | Notes
---|---|---
PK | tenant |
CK | series_set_hash | Hash as base64 text
&nbsp; | metric_name | 
&nbsp; | tags | map<text,text>

`series_sets`:

&nbsp; | Name | Notes
---|---|---
PK | tenant |
PK | metric_name | 
CK | tag_key |
CK | tag_value |
CK | series_set_hash | 

`metric_groups`:

&nbsp; | Name | Notes
---|---|---
PK | tenant |
CK | metric_group | 
&nbsp;   | metric_names | Set of metric names for a metric_group
&nbsp;   | updated_at |


`devices`:

&nbsp; | Name | Notes
---|---|---
PK | tenant |
CK | device | 
&nbsp;   | metric_names | Set of metric names for a device
&nbsp;   | updated_at |


`tags_data`:

&nbsp; | Name | Notes
---|---|---
PK | tenant |
PK | type | Type can be either TAGK or TAGV
CK | data | value of TAGK or TAGV

#### Raw/Ingested Data

`data_raw_p_{timeSlotWidth}`:

&nbsp; | Name | Notes
---|---|---
PK | tenant |
PK | time_slot | Configurable-width time slot for partitioning
CK | series_set_hash | 
CK | ts | timestamp of ingested metric
&nbsp; | value | Metric value as double

To be a bit more explicit, all these tables are updated/inserted on ingest.  The "metric_names" table is read during metadata retrieval of tenants and metric names. Metadata queries for tag keys and values given tenant and metric name are resolved by the "series_sets" table. The "series_sets" and "data_raw" tables are used in combination by data queries and "data_raw" is also used during downsampling.

The `time_slot` column is a configurable-width, normalized time slot column that optimizes for Cassandra's use of the bloom filter when locating the sstables supporting a particular query, especially the downample queries. For the raw data table it is ideal to set the width of the time slot the same as downsample width since it minimizes the number of sttables consulted on downsample.

### Series-Set

Each metric value is identified by a combination of timestamp, tenant, and series-set. A series-set is a combination of the metric name and tags where the tags are sorted by tag key to ensure consistent evaluation.

Since the textual encoding of a series-set could vary greatly in length due to the number and complexity of tags in the ingested data, most of the tables store a hash string of the series-set, named as `series_set_hash`. The stored hash string is computed as follows:
1. Sort the tags by key
2. Compute a 128-bit murmur3 hash of the metric name and sorted tags
3. Convert the 128-bit hash value into a Base64 string of 24 characters

The series-sets metadata is tracked using a combination of the `series_sets` and `series_set_hashes` tables where the two tables cross-index into each other in order to satisfy various metadata retrievals.

It is worth noting that Ceres follows the simpler metric-value philosophy that is used by [Prometheus](https://prometheus.io/docs/concepts/data_model/), [OpenTSDB](http://opentsdb.net/docs/build/html/user_guide/writing/index.html#metrics-and-tags), and others. Even Grafana's query retrieval works with a metric-value at a time.
                                                                          
This approach tends to be most portable, because metric-field-value structured metrics, like telegraf's/InfluxDB can  be denormalized into metric-value by forming a concatenated `{metric}_{field}` named metric. For example, looking at [telegraf's cpu metrics](https://github.com/influxdata/telegraf/tree/master/plugins/inputs/cpu), the qualified metric names become `cpu_time_user`, `cpu_time_system`, ... `cpu_usage_guest_nice`.

### Ingest

When ingesting a metric, the following actions occur:
1. A series-set hash is computed from the metric name and tags, [as described above](#series-set)
2. The data value itself is inserted into the `data_raw` table
3. The metadata is updated, as follows:
   - An in-memory cache is consulted given the key tenant + series-set hash. If that cache entry is present, then (referring to the last step, below) related metadata must already be present in cassandra and the following steps are skipped
   - A Redis [`SETNX`](https://redis.io/commands/setnx) (SET if Not eXists) operation is performed given the tenant + series-set hash. If the key was present, then the following steps are skipped.
     
     NOTE: Redis is used as an intermediate caching layer to ensure that rolling upgrades of Ceres replicas can share the previous lookups as the in-memory cache is warmed up in each replica
   - With the series-set hash and its constituent parts, the `series_set_hashes` table is `INSERT`ed
   - The following metadata is also `INSERT`ed:
     - A row for each tag key-value is inserted into `series_sets`
     - A row for the metric name is inserted into `metric_names`
     - Or updated the set of metric names for a metric_group in `metric_groups` table
     - Or updated the set of metric names for a device in `devices` table
     - A row for TAGK and TAGV is inserted into `tags_data`
     
     NOTE: even if cache misses of both in-memory and Redis were false indications, then re-insertion of metadata is harmless since Cassandra will resolve an INSERT into an existing row as if it was an UPDATE
   - The steps above result the tenant + series-set hash entry to be populated in the in-memory cache 

### Data Query

The goal of a data query is to provide the data for the requested tenant, metric name, and one or more tags whose timestamped values fall within the requested time range

Such a query is performed using the following steps:
- Given the tenant, metric name, and tags, query the `series_sets` table to retrieve the applicable series-set hashes 
- Use the series-set hashes to query the `data_raw` table to retrieve the values that fall within the requested time range
- For each matching series-set, the response includes a result set composed of
  - A list of timestamp-value pairs
  - The metric name
  - The full tags from the series-set. The `series_set_hashes` table is used to locate those tags given the hash from the first step

As an example, let's say the user's query is requesting the metric `cpu_idle` for Linux servers in production by specifying the tags `os=linux` and `deployment=prod`. 

A retrieval from `series_sets` for `os=linux` could be:

| tenant | metric\_name | tag\_key | tag\_value | series\_set\_hash |
| :--- | :--- | :--- | :--- | :--- |
| t-1 | cpu\_idle | os | linux | hash3 |
| t-1 | cpu\_idle | os | linux | hash1 |
| t-1 | cpu\_idle | os | linux | hash4 |

> NOTE: the hash strings would not normally look like "hash3", etc but instead would be Base64 encoded strings

...and for `deployment=prod` could be:

| tenant | metric\_name | tag\_key | tag\_value | series\_set |
| :--- | :--- | :--- | :--- | :--- |
| t-1 | cpu\_idle | deployment | prod | hash1 |
| t-1 | cpu\_idle | deployment | prod | hash2 |
| t-1 | cpu\_idle | deployment | prod | hash4 |

The series-sets to retrieve from the data table can then be computed by finding the intersection of the resulting hashes. With the example above, that intersection would be:

- `hash1`
- `hash4`

The next step is achieved by querying the data table for each series-set hash, tenant, and the requested time-range. Continuing the example, those rows from `data_raw` could be:

| tenant | series\_set | ts | value |
| :--- | :--- | :--- | :--- |
| t-1 | hash4 | 2020-08-24 16:34:05.000 | 477 |
| t-1 | hash1 | 2020-08-24 15:51:15.000 | 186 |
| t-1 | hash1 | 2020-08-24 16:23:54.000 | 828 |
| t-1 | hash1 | 2020-08-24 16:23:58.000 | 842 |
| t-1 | hash1 | 2020-08-24 16:26:52.000 | 832 |
| t-1 | hash1 | 2020-08-24 16:34:05.000 | 436 |

Finally, the full set of tags to include in the result sets are queried from `series_set_hashs`, such as:

| tenant | series\_set\_hash | metric\_name | tags |
| :--- | :--- | :--- | :--- |
| t-1 | hash4 | cpu\_idle | {deployment=prod, host=h-4, os=linux} |
| t-1 | hash4 | cpu\_idle | {deployment=prod, host=h-1, os=linux} |

The user's query response would then be rendered as:

```json
[
  {
    "tenant": "t-1",
    "metricName": "cpu_idle",
    "tags": {
      "host": "h-4",
      "os": "linux",
      "deployment": "prod"
    },
    "values": {
      "2020-08-24T16:34:05Z": 477.0
    }
  },
  {
    "tenant": "t-1",
    "metricName": "cpu_idle",
    "tags": {
      "host": "h-1",
      "os": "linux",
      "deployment": "prod"
    },
    "values": {
      "2020-08-24T15:51:15Z": 186.0,
      "2020-08-24T16:23:54Z": 828.0,
      "2020-08-24T16:23:58Z": 842.0,
      "2020-08-24T16:26:52Z": 832.0,
      "2020-08-24T16:34:05Z": 436.0
    }
  }
]
```

### Downsampling (a.k.a. roll-ups, normalized)

#### During ingestion

Downsampling is the process of aggregating "raw" metrics, which are collected and conveyed at arbitrary timestamps, into deterministic time granularities, such as 5 minute and 1 hour. The intention is to retain aggregated metrics for much longer periods of time than the raw metrics. Downsampling also benefits queries with wider time ranges since it reduces the number of data points to be retrieved and rendered. 

This process is also known as roll-up since it can be thought of finer grained data points rolling up into wider and wider grained levels of data. It is also referred to as normalized since the result of downsampling allows related metrics to be compared in time since the timestamps would align consistently even if those metrics were originally collected slightly "out of sync". 

- Downsampling work is sliced into partitions, much like Cassandra's partitioning key concept
- Partitions are an integer value in the range \[0, partition-count\)
- A particular partition value is computed by hashing the tenant and series-set of a metric and then applycing a consistent hash, such as [the one provided by Guava](https://guava.dev/releases/21.0/api/docs/com/google/common/hash/Hashing.html#consistentHash-com.google.common.hash.HashCode-int-)
- The application is configured with a "time slot width", which is used to define unit of tracking and the range queried for each downsample operation.
- As a raw metric is ingested, the downsampling time slot is computed by rounding down the metric's timestamp to the next lowest multiple of the time slot width. For example, if the time slot width is 1 hour, then rounded timestamp of the downsample set would always be at the top-of-the-hour.
- The time slot width must be a common-multiple of the desired granularities. For example, if granularities of 5 minutes and 1 hour are used, then the time slot width must a multiple of an hour. With a one-hour time slot width, 12 5-minute downsamples would fit and one 1-hour downsample. 

#### Redis pending downsample set tracking

The following keys are used in Redis to track the pending downsample sets:

- `ingesting|<partition>|<slot>` : string
- `pending|<partition>|<slot>` : set

The ingestion process updates those entries as follows:
- Compute `partition` as `hash(tenant, seriesSetHash) % partitions`
- Compute `slot` as the timestamp of metric normalized to `timeSlotWidth`. It is encoded in the key as the epoch seconds.
- Redis `SETEX "ingesting:<partition>:<slot>"` with expiration of 5 minutes (configurable)
- Redis `SADD "pending:<partition>:<slot>" "<tenant>:<seriesSetHash>"`

Notes:
- Expiration could also take into consideration that a downsample time slot that overlaps with the current time should also wait to expire until the end of the time slot and add the configurable duration

#### Downsample processing

- Each replica is configured with a distinct set of partition values that it will process
- Downsample processing is an optional function of this application and can be disabled by not specifying any partitions to be owned and processed. This allows for deploying the same application code as both a cluster of ingest/query nodes, a cluster of downsample processors, or a combination thereof.

The following diagram shows the high level relationship of raw metrics in a given downsample slot to the aggregated time slots:

![](docs/downsample-timeline.drawio.png)

> The "m" indicators represent raw metric values with their original timestamp.
> The timeline is marked with an example granularity set of 5 minutes (5m) and 1 hour (1h).
> The segment markers below the timeline indicate target granularities, where the bottom-most, solid segment indicates the downsample time slot width to be processed.

The following introduces the table structure for each downsampled data table. The `{granularity}` in the table name is the granularity's ISO-8601 format as a lowercase string, such as `pt5m`. A separate table is used per granularity in order to ensure Cassandra efficiently processes the default TTL per table using the [Time Window Compaction Strategy](https://cassandra.apache.org/doc/latest/operating/compaction/twcs.html#twcs).

`data_{granularity}_p_{timeSlotWidth}` table:

&nbsp; | Name | Notes
-------|------|------
PK | tenant |
PK | time_slot | Configurable-width time slot for partitioning
CK | series_set_hash | 
CK | aggregator | One of min,max,sum,count,avg
CK | ts | timestamp rounded down by granularity
&nbsp; | value | 

For each `partition`:
- Schedule periodic retrieval of pending downsample set tracking info:
  - Redis `SCAN <cursor> MATCH "pending:<partition>:*"`
  - For each result
    - Extract `slot` suffix from key
    - If not `EXISTS "ingesting:<partition>:<slot>"`
      - `SSCAN "pending:<partition>:<slot>"` over entries in time slot
      - Process pending downsample since value contained `tenant` and `seriesSetHash`
      - `SREM` the value from the respective pending key, where last `SREM` will remove the entire key
  - Repeat `SCAN` until cursor returns 0

- Each ready downsample set is used to determine the raw data query to perform since it includes tenant, series-set, `time_slot` as the start of the time range and `time_slot` + slot width as end of time range.
- The following aggregations are considered for each downsample:
    - min
    - max
    - sum
    - average
- A configurable list of "counter suffixes" is used to identify from the metric name the raw metrics that should only be aggregated with a sum-aggregation. Examples of such suffixes are "reads", "writes", "bytes"
- All other metrics will be treated as gauges and aggregated with min, max, and average. The average of each granularity slot is computed from `sum` / `count`, where `count` is aggregated by slot but not persisted.
- The downsample process will iterate through the configured aggregation granularities and aggregate the values of each time slot from the finer granularity values before it, starting with the raw data stream. The following diagram relates that process into the flux returned by the Reactive Cassandra query and the flux instances created for each aggregation granularity:

![](docs/downsample-aggregation-flow-diagram.drawio.png)

- As shown on the right hand side of the diagram above, each granularity slot is ultimately gathered into an update-batch
- Each row in the update-batch will include the tenant, series-set, and normalized timestamp.
    - It's important that the aggregate data row be `UPDATE`d rather than `INSERT`ed to accommodate late arrival handling, describe below. To ruin the surprise, the aggregated metric values are upserted in order to allow for re-calculation later and re-upserting the new value.
    - The TTL used for each update-batch will be determined by the retention duration configured for that granularity. For example, 5m granularity might configured with 14 days retention and 1h with 1 year retention.
- The `metric_names` table is also upserted with the original raw metric's name and the aggregators identified during the aggregation process. For example, a gauge with the name `cpu_idle` could start with an `aggregators` of only `{'raw'}`, but after aggregation processing would become `{'raw','min','max','avg'}`

#### Processing late arrivals

Late arrivals of metrics into `data_row` are an interesting case to confirm are covered by the strategy above. There are two versions of late arrivals:

- No metrics at all showed up for a given tenant+series-set during a time slot. This case is handled by the design above since:
    - The redis ingesting/pending entries would not have been created at the original time
    - When those metrics arrive later, the redis entries are created
    - Once the readiness of the downsample set is satisfied it is processed as normal
- A subset of metrics showed up for a given tenant+series-set at the original time
    - The downsample processor won't know that there's a subset of the expected metrics; however, that's fine because all of the aggregations are mathematically correct for what values are present. For example, the "count" aggregation would be mathematically accurate, even if technically lower than a user would expect.
    - When the remaining metrics arrive later, the redis entries are **re-created**
    - Once the readiness of the downsample set is satisfied the downsample processor will pick up on the redis entries at the next scheduled time, the re-query of that time slot's raw data will now retrieve all expected metrics entries. _This assumes the raw data for that queried time slot has not been TTL'ed away._
    - The downsample processor will aggregate the granularities entries as described above and again upsert/UPDATE the resulting update-batches as usual. With the UPDATE the "partial" aggregation values will be replaced by the "complete" aggregation values

#### Scheduling downsampling jobs

![](docs/downsampling-sequence.png)

* The partition space is subdivided into 4 jobs: 1,2,3,4.
* Each job consists of 16 partitions.
* Each job is scheduled to run every `downsample-process-period` apart.
* Every time a job is run, a new timestamp is saved in Redis for next future job.
* The job is then scheduling each partition to run at equal distances apart during the length of the process period.

#### Ceres pods and the competing consumer pattern

![](docs/consumers.png)

* Each ceres pod is constantly polling the Redis database to see if a downsampling job is available to run based on the
timestamps.
* Since each ceres kubernetes pod is competing for each job on a first-come-first-served basis it will provide the randomness
required to equally distribute the work load over all the ceres pods.
* Since the work load is equally distributed over all the pods, it will still distribute the load even if a pod is taken
down or added i.e. in the auto-scaling scenario.

## Spring Webflux / Project Reactor Overview

The Ceres application makes heavy use of Reactive Spring features including:
- Spring Webflux for serving API calls
- Reactive Spring Data Cassandra
- Reactive Spring Data Redis

The following are some helpful links to learn about the concepts used in Ceres:
- [Spring Boot Webflux docs](https://docs.spring.io/spring-boot/docs/current/reference/html/spring-boot-features.html#boot-features-webflux)
- [Core Spring Webflux docs](https://docs.spring.io/spring-framework/docs/current/reference/html/web-reactive.html#spring-webflux)
- [Spring Data Cassandra, Reactive support](https://docs.spring.io/spring-data/cassandra/docs/current/reference/html/#cassandra.reactive)
- [Spring Data Redis, Reactive support](https://docs.spring.io/spring-data/redis/docs/current/reference/html/#redis:reactive)
- [Project Reactor reference docs](https://projectreactor.io/docs/core/release/reference/)
- [Project Reactor operator summary](https://projectreactor.io/docs/core/release/reference/docs/index.html#which-operator)
- [Flux](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html) - and [Mono](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html) javadoc
- [Reactive Streams](https://www.reactive-streams.org/) APIs all boil down to [publisher](https://www.reactive-streams.org/reactive-streams-1.0.3-javadoc/org/reactivestreams/Publisher.html) and [subscriber](https://www.reactive-streams.org/reactive-streams-1.0.3-javadoc/org/reactivestreams/Subscriber.html)
