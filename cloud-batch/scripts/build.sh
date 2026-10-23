#!/usr/bin/env bash

set -euo pipefail
cd "$(dirname "$0")/../jobs"

docker run --rm \
  -v "$PWD":/project -w /project \
  -v sedona-sbt-cache:/root/.cache \
  sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.11.7_2.13.17 \
  sbt -batch scalafmtAll clean package

ls -l target/sedona-jobs.jar
