#!/usr/bin/env bash

set -euxo pipefail

sudo mkfs -t xfs /dev/sdb
sudo mkdir /data
sudo mount /dev/sdb /data
sudo chown -R ec2-user:ec2-user /data
