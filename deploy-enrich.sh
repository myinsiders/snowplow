#!/usr/bin/env bash

cd ~/Documents/WS/digdeep-snowplow/3-enrich/scala-common-enrich
sbt publishLocal || exit 1
cd ~/Documents/WS/digdeep-snowplow/3-enrich/scala-hadoop-enrich
sbt assembly || exit 1
aws --profile digdeep-snowplow s3 cp target/scala-2.10/snowplow-hadoop-etl-0.12.0.jar s3://digdeep-snowplow-hosted-assets/emr/snowplow-hadoop-etl-0.12.0_1.jar
