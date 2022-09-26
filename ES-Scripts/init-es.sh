#!/bin/bash
# init-es.sh is the script to be run to properly setup the es cluster.  The main things
#  it does is set up the aliases and mappings as required by BF.  The mappings
#  are required to properly tokenize the incoming data.
# You can pass it a url if your elastic search doesn't reside on localhost:9092
#WARNING: this script will destroy existing ES data and reset it to the proper init state
# Example:
#    Local ES: ./init-es.sh
#    ES on OR: ./init-es.sh -u <url for OR:ES> -n <username> -p <password>

# Set default ES URL
ELASTICSEARCH_URL=http://localhost:9200

usage() {
    echo "Usage: $0 [-u <remote ES url:string>(default: localhost:9200)] [-n <username:string>] [-p <password:string>] " 1>&2;
    exit 1
}

while getopts "p:u:n:" o; do
    case "${o}" in
        p) ES_PASSWD=$OPTARG ;;
        u) ELASTICSEARCH_URL=$OPTARG ;;
        n) ES_USERNAME=$OPTARG ;;
        *) usage ;;
    esac
done

# Set a auth header for curl if username and passwd is supplied
# Allows initing ES when ES is supplied by a SaaS provider like ObjectRocket
if [ $ES_USERNAME ] && [ $ES_PASSWD ]; then
    AUTH="-k -u ${ES_USERNAME}:${ES_PASSWD}"
fi

checkFile metrics-mapping.json
checkFile metris-alias.json

curl $AUTH -H 'Content-Type: application/json' -XPUT $ELASTICSEARCH_URL'/metric_metadata' -d@metrics-mapping.json
curl $AUTH -H 'Content-Type: application/json' -XPUT $ELASTICSEARCH_URL'/_aliases?pretty' -d@metris-alias.json
