### Set up minikube with ceres pods

* Modify src/main/java/resources/application.yml to look like this:
```yaml
ceres:
  downsample:
    granularities:
      - width: 1m
        ttl: 12h
        partitionWidth: 2h
      - width: 2m
        ttl: 1d
        partitionWidth: 4h
    time-slot-width: 2m
    last-touch-delay: 1m
    downsample-process-period: 10s
    partitions: 4
    partitions-to-process: 0-3
  tag-filter: truncate
  tag-value-limit: 50
  excluded-tag-keys: ["ip_address"]
  ingest-start-time: 7d
  ingest-end-time: 1d
spring:
  redis:
    database: 0
    port: 6379
    host: 192.168.49.1 # Check your localhost ip address
  data:
    cassandra:
      keyspace-name: ceres
      schema-action: create_if_not_exists
      local-datacenter: datacenter1
      port: 9042
      contact-points: 192.168.49.1 # Check your localhost ip address
```
* Install minikube
* Start minikube
```shell script
minikube start
```
* Set kubernetes docker environment
```shell script
eval $(minikube docker-env)
```
* Build ceres
```shell script
./mvnw clean package -DskipTests
```
* Build ceres docker image
```shell script
docker build -t ceres-in-docker:0.5 .
```
* Check that the image was built
```shell script
docker images
```
* Create ceres kubernetes deployment
```shell script
kubectl apply -f deployment-local.yaml
```
* Create service and expose external port
```shell script
kubectl expose deployment ceres --type=NodePort
```
* Get URL for service
```shell script
minikube service --url ceres
```
You should see something like this:
http://192.168.49.2:31053

* Try doing a curl to ceres
```shell script
curl -H "X-Tenant: t-1" http://192.168.49.2:31053/api/metadata/metricNames
```
* Some diagnostic commands:
```shell script
kubectl get pods
kubectl get deployments ceres
kubectl get services ceres
kubectl describe deployments ceres
```
* Clean up commands
```shell script
kubectl delete deployment ceres
kubectl delete service ceres
minikube stop # graceful stop
minikube delete --all # this will delete the whole cluster
```
