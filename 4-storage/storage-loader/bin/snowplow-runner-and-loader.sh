#!/bin/bash

# Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

# Update these for your environment
RUNNER_PATH=/home/ubuntu/snowplow/3-enrich/emr-etl-runner
LOADER_PATH=/home/ubuntu/snowplow/4-storage/storage-loader
RUNNER_CONFIG=/home/ubuntu/snowplow/3-enrich/emr-etl-runner/config/config.yml
RUNNER_ENRICHMENTS=/home/ubuntu/snowplow/3-enrich/emr-etl-runner/config/enrichments
LOADER_CONFIG=/home/ubuntu/snowplow/4-storage/storage-loader/config/redshift.yml
which rvm || source /etc/profile.d/rvm.sh

# Run the ETL job on EMR
export BUNDLE_GEMFILE=${RUNNER_PATH}/Gemfile
bundle exec ${RUNNER_PATH}/bin/snowplow-emr-etl-runner --config ${RUNNER_CONFIG} --enrichments ${RUNNER_ENRICHMENTS} $*

# Check the damage
ret_val=$?
if [ $ret_val -ne 0 ]; then
    echo "Error running EmrEtlRunner, exiting with return code ${ret_val}. StorageLoader not run"
    exit $ret_val
fi

# If all okay, run the storage load too
export BUNDLE_GEMFILE=${LOADER_PATH}/Gemfile
bundle exec ${LOADER_PATH}/bin/snowplow-storage-loader --config ${LOADER_CONFIG}
