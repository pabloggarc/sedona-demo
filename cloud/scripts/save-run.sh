#!/usr/bin/env bash
#
# Spark keeps its event logs in the HDFS of the cluster, so they are gone after a destroy. This
# copies them to the bucket and downloads them into runs/events, where the spark-history service of
# the local part opens them, and the EMR logs into runs/logs/<cluster>. Run it before a destroy.
#
#   ./scripts/save-run.sh

set -euo pipefail
cd "$(dirname "$0")/.."

export AWS_REGION=$(terraform -chdir=terraform output -raw region)
temp_uri=$(terraform -chdir=terraform output -raw temp_uri)
cluster=$(terraform -chdir=terraform output -raw cluster_id)

step=$(aws emr add-steps --cluster-id "$cluster" \
  --steps "Type=CUSTOM_JAR,Name=save-event-logs,ActionOnFailure=CONTINUE,Jar=command-runner.jar,Args=[s3-dist-cp,--src,hdfs:///var/log/spark/apps,--dest,$temp_uri/spark-events/$cluster/]" \
  --query 'StepIds[0]' --output text)
aws emr wait step-complete --cluster-id "$cluster" --step-id "$step"

aws s3 sync --quiet "$temp_uri/spark-events/$cluster/" runs/events/
aws s3 sync --quiet "$temp_uri/logs/$cluster/" "runs/logs/$cluster/" --exclude "node/*"

echo "Saved in runs/events and runs/logs/$cluster"
