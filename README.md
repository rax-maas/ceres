A simple multi-dimensional, time-series ingest and query service backed by Cassandra.

## Quickstart

Startup a Cassandra container:

```shell script
docker-compose up -d
```

Watch the container logs until it reports "Starting listening for CQL clients on /0.0.0.0:9042":
```shell script
docker-compose logs -f cassandra
```

Create the keyspace:
```shell script
docker-compose exec cassandra \
  cqlsh -e "CREATE KEYSPACE IF NOT EXISTS tsdb WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
```

Start the ingest/query application:
```shell script
mvn spring-boot:run
```

### Write some data

```http request
POST http://localhost:8080/api/write/single
Content-Type: application/json

{
  "tenant": "t-1",
  "metricName": "cpu_idle",
  "tags": {
    "os": "linux",
    "host": "h-1",
    "deployment": "prod"
  },
  "ts": {{$timestamp}},
  "value": {{$randomInt}}
}
```

### Query metadata

Metric names, such as
```http request
GET http://localhost:8080/api/metadata/metricNames?tenant=t-1
```

Tag keys, such as
```http request
GET http://localhost:8080/api/metadata/tagKeys?tenant=t-1&metricName=cpu_idle
```

Tag values, such as
```http request
GET http://localhost:8080/api/metadata/tagValues?
  tenant=t-1
  &metricName=cpu_idle
  &tagKey=os
```

### Query data

```http request
GET http://localhost:8080/api/query?
  tenant=t-1
  &metricName=cpu_idle
  &tag=os=linux
  &start=2020-08-23T17:53:00Z
  &end=2020-08-23T17:54:40Z
```

Responds with query results per series-set, such as:
```json
[
  {
    "tenant": "t-1",
    "metricName": "cpu_idle",
    "tags": {
      "host": "h-1",
      "os": "linux",
      "deployment": "prod"
    },
    "values": {
      "2020-08-24T00:13:16Z": 491.0,
      "2020-08-24T00:13:20Z": 792.0,
      "2020-08-24T00:13:21Z": 824.0
    }
  },
  {
    "tenant": "t-1",
    "metricName": "cpu_idle",
    "tags": {
      "host": "h-3",
      "os": "linux",
      "deployment": "dev"
    },
    "values": {
      "2020-08-24T00:15:52Z": 84.0,
      "2020-08-24T00:15:55Z": 498.0
    }
  }
]
```

## Design

### Tables

The Cassandra tables in the `tsdb` keyspace (by default) are organized into two categories:
- **metadata** : tables that enable metadata lookup directly or to enable queries
- **data** : tables that store the timestamped values

![](docs/tables.drawio.png)

### Series-Set

Each series is identified by a "series-set" which is a compact, textual, stable representation of a series.

The syntax is:
```
{metric_name},{tag_key}={tag_value,{tag_key}={tag_value},...
```

where the tag key-value pairs are sorted by tag key to ensure a stable and deterministic structure.

The `series_sets` table is effectively an index of each `tag_key`-`tag_value` pair to the one or more series sets that contain that tag pair.

### Ingest

When ingesting a metric, the following actions occur:
1. A series-set is computed from the metric name and tags, [as described above](#series-set)
2. The data value itself is inserted
3. The metadata of the metric series is "upserted". 
   - Cassandra doesn't have first-class support for upserts; however, the metadata tables consist entirely of primary key columns and a CQL `INSERT` differs from SQL in that it will ensure "the row is created if none existed before, and updated otherwise"
   - Four tables are involved in the metadata upsert in order to accommodate the denormalized/NoSQL nature of Cassandra and enable metadata retrieval by each facet

### Data Query

The goals of a data query are
- retrieve the data for the requested tenant, metric name, and one or more tags
- provide the timestamped values that fall within the requested time range

The first goal is achieved by retrieving the list of series-sets for each request tag (key-value). For example, let's say the query is requesting the metric `cpu_idle` and results for the tags `os=linux` and `deployment=prod`. A metadata retrieval of series-sets for each tag would be:

For `os=linux`:

| tenant | metric\_name | tag\_key | tag\_value | series\_set |
| :--- | :--- | :--- | :--- | :--- |
| t-1 | cpu\_idle | os | linux | cpu\_idle,deployment=dev,host=h-3,os=linux |
| t-1 | cpu\_idle | os | linux | cpu\_idle,deployment=prod,host=h-1,os=linux |
| t-1 | cpu\_idle | os | linux | cpu\_idle,deployment=prod,host=h-4,os=linux |

For `deployment=prod`:

| tenant | metric\_name | tag\_key | tag\_value | series\_set |
| :--- | :--- | :--- | :--- | :--- |
| t-1 | cpu\_idle | deployment | prod | cpu\_idle,deployment=prod,host=h-1,os=linux |
| t-1 | cpu\_idle | deployment | prod | cpu\_idle,deployment=prod,host=h-2,os=windows |
| t-1 | cpu\_idle | deployment | prod | cpu\_idle,deployment=prod,host=h-4,os=linux |

The series-sets to retrieve from the data table can then be computed by finding the intersection of `series_set` from all of the tag retrievals. With the example above, that intersection would be:

- `cpu_idle,deployment=prod,host=h-1,os=linux`
- `cpu_idle,deployment=prod,host=h-4,os=linux`

The second goal of the query is achieved by iterating over each series-set and querying the data table by tenant, series-set, and the requested time-range. Continuing the example, those rows from `data_raw` would be:

| tenant | series\_set | ts | value |
| :--- | :--- | :--- | :--- |
| t-1 | cpu\_idle,deployment=prod,host=h-4,os=linux | 2020-08-24 16:34:05.000 | 477 |
| t-1 | cpu\_idle,deployment=prod,host=h-1,os=linux | 2020-08-24 15:51:15.000 | 186 |
| t-1 | cpu\_idle,deployment=prod,host=h-1,os=linux | 2020-08-24 16:23:54.000 | 828 |
| t-1 | cpu\_idle,deployment=prod,host=h-1,os=linux | 2020-08-24 16:23:58.000 | 842 |
| t-1 | cpu\_idle,deployment=prod,host=h-1,os=linux | 2020-08-24 16:26:52.000 | 832 |
| t-1 | cpu\_idle,deployment=prod,host=h-1,os=linux | 2020-08-24 16:34:05.000 | 436 |

The resulting JSON structure derives the metric name and tag-map by decomposing the series-sets of the results and combining each with the timestamp-values. For example:

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
