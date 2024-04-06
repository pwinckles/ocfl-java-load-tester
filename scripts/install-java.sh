#!/usr/bin/env bash

set -euxo pipefail

wget https://corretto.aws/downloads/latest/amazon-corretto-21-x64-linux-jdk.tar.gz
tar xzf amazon-corretto-21-x64-linux-jdk.tar.gz
rm amazon-corretto-21-x64-linux-jdk.tar.gz
mv amazon-corretto-* java
