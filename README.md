A simple multi-dimensional, time-series ingest and query service backed by Cassandra. Redis is also used for tracking of downsample operations.

## Quickstart

Startup Cassandra and Redis containers:

```shell script
docker-compose up -d
```

Watch the Cassandra container logs until it reports "Starting listening for CQL clients on /0.0.0.0:9042":
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
POST http://localhost:8080/api/put
Content-Type: application/json

{
  "metric": "cpu_idle",
  "tags": {
    "tenant": "t-1",
    "os": "linux",
    "host": "h-1",
    "deployment": "prod"
  },
  "timestamp": {{$timestamp}},
  "value": {{$randomInt}}
}
```

where the above IntelliJ HTTP request snippet substitutes current epoch seconds for `{{$timestamp}}` and a random integer value at `{{$randomInt}}`.

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

### Ingesting data from telegraf

The following telegraf config snippet can be used to output metrics collected by telegraf into `tsdb-cassandra`:

```toml
[[outputs.opentsdb]]
  ## prefix for metrics keys
  # prefix = "my.specific.prefix."
  host = "http://localhost"
  port = 8080
  http_batch_size = 50
  http_path = "/api/put"
  debug = false
  separator = "_"
```

### Downsampling

Continuous downsampling is configured in the `app.downsample` application properties, as shown in the following example:

```yaml
app:
  downsample:
    # For tracking during ingest
    partitions: 4
    time-slot-width: 2m

    # For downsample processing
    partitions-to-process: 0-3
    last-touch-delay: 1m
    downsample-process-period: 10s
    granularities:
      - width: 1m
        ttl: 12h
      - width: 2m
        ttl: 24h
```

Querying for downsample data uses the same endpoint as raw data; however, the addition of `aggregator` and `granularity` indicate the use of downsample data. The following is an example of a querying for downsampled data with 'min' aggregation at 2-minute granularity:

```http request
GET http://localhost:8080/api/query?
  tenant=t-1
  &metricName=cpu_idle
  &aggregator=min
  &granularity=PT2M
  &tag=os=linux
  &tag=deployment=prod
  &start=2020-09-15T16:00:00Z
  &end=2020-09-15T17:00:00Z
```

## Design

Design documentation is available in [DESIGN.md](DESIGN.md).