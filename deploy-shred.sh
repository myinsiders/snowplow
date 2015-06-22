#!/usr/bin/env bash

cd ~/Documents/WS/digdeep-snowplow/3-enrich/scala-common-enrich
sbt publishLocal || exit 1
cd ~/Documents/WS/digdeep-snowplow/3-enrich/scala-hadoop-shred
sbt assembly || exit 1
time s4cmd -p ~/.aws/s3.cfg put -f target/scala-2.10/snowplow-hadoop-shred-0.3.0.jar s3://digdeep-snowplow-hosted-assets/scala-hadoop-shred/snowplow-hadoop-shred-0.3.0_1.jar
