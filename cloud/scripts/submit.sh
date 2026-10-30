#!/usr/bin/env bash
#
# Uploads the jar of the jobs to the bucket and runs one of its classes on the EMR cluster as a
# step. It returns right away, and the step keeps running on the cluster.
#
#   ./scripts/submit.sh sedonademo.ObsMunicipality ES

set -euo pipefail
cd "$(dirname "$0")/.."

target=$1
shift
export AWS_REGION=$(terraform -chdir=terraform output -raw region)
code_uri=$(terraform -chdir=terraform output -raw code_uri)
temp_uri=$(terraform -chdir=terraform output -raw temp_uri)
cluster=$(terraform -chdir=terraform output -raw cluster_id)

aws s3 cp --quiet jobs/target/sedona-jobs.jar "$code_uri/jars/sedona-jobs.jar"
args="spark-submit,--class,$target,$code_uri/jars/sedona-jobs.jar${*:+,$(IFS=,; echo "$*")}"

step=$(aws emr add-steps --cluster-id "$cluster" \
  --steps "Type=CUSTOM_JAR,Name=$target,ActionOnFailure=CONTINUE,Jar=command-runner.jar,Args=[$args]" \
  --query 'StepIds[0]' --output text)

echo "Step $step submitted to $cluster"
echo "Logs: $temp_uri/logs/$cluster/steps/$step/"
