# Current status of CERES project

## Useful links
* https://github.com/rax-maas/ceres
* https://rackspace.atlassian.net/jira/software/c/projects/CERES/boards/5082
* https://console.cloud.google.com/kubernetes/workload/overview?authuser=0&project=ceres-dev-222017&pageState=(%22savedViews%22:(%22i%22:%2204ab195c8b704ec083f8b28bdcd18824%22,%22c%22:%5B%5D,%22n%22:%5B%22default%22%5D))
* https://ceres-grafana.dev.monplat.rackspace.net/d/pzwMpin7k/ceres-apps-new?orgId=1&refresh=30s
* https://ceres-grafana.dev.monplat.rackspace.net/d/3oNtmckMk/redis-golden-signals?orgId=1&from=now-30m&to=now
* https://ceres-grafana.dev.monplat.rackspace.net/d/9VfDrq97z/cassandra-overview?orgId=1&refresh=30s&from=now-3h&to=now
* TODO: Suman/Rohit/Abhishek

## Current status

### Overall status
* TODO: Suman/Rohit/Abhishek

#### General TODO
* Test CERES with higher ingest loads 2-5M/minute
* Split CERES into 3 separate services in GCP
  * ceres-ingest
  * ceres-rollups
  * ceres-query
* TODO: Suman/Rohit/Abhishek
* Look over the search api how we can deal with the requirement in https://rackspace.atlassian.net/browse/MNRVA-987 by maybe customizing this in search api only or creating a seperate api for this.

### Ingestion
* TODO: Suman/Rohit/Abhishek

#### Ingestion TODO
* Make ElasticSearch more performant for higher loads
* Establish a TTL strategy for ElasticSearch
* Store tag combination hashes in 1 place only, now we are storing them in 3 places: seres_set_hashes, series_sets, downsampling_hashes

### Downsampling
* Regularly scheduled metrics downsampling PT5M, PT15M, PT1H,... are running well with high performance
* Delayed metrics downsampling is relying a lot on Redis for frequent updates and is running well with high performance

#### Downsampling TODO
* Possibly create a delayed metrics schedule for better spacing out in time for delayed metrics, currently they are downsampled as soon as possible.
* Test performance with higher ingestion load 2-5M/minute
