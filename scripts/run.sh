#!/usr/bin/env bash

set -euxo pipefail

HOME=/home/ec2-user

mkdir -p temp test
screen "${HOME}/java/bin/java" -jar "${HOME}/ocfl-java-load-tester-1.0.0-SNAPSHOT-exec.jar" "$@"
