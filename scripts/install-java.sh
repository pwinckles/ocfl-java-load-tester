#!/usr/bin/env bash

set -euxo pipefail

wget https://corretto.aws/downloads/latest/amazon-corretto-17-x64-linux-jdk.tar.gz
tar xzf amazon-corretto-17-x64-linux-jdk.tar.gz
rm amazon-corretto-17-x64-linux-jdk.tar.gz
mv amazon-corretto-17.0.6.10.1-linux-x64 java
