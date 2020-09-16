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

_Documentation coming soon, but see [application-dev.yml](src/main/resources/application-dev.yml) for an example. Example of a query for downsampled data of 'min' aggregation at 2m granularity is:_

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

