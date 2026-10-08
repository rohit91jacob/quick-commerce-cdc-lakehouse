#!/bin/sh
# Single-process SeaweedFS (master + volume + filer + S3 gateway) with one S3 identity from the environment.
set -eu
cat > /tmp/s3.json <<JSON
{"identities": [{"name": "lakehouse", "credentials": [{"accessKey": "${S3_ACCESS_KEY}", "secretKey": "${S3_SECRET_KEY}"}],
  "actions": ["Admin", "Read", "Write", "List", "Tagging"]}]}
JSON
exec weed server -dir=/data -ip=seaweedfs -ip.bind=0.0.0.0 -s3 -s3.port=8333 -s3.config=/tmp/s3.json \
  -master.volumeSizeLimitMB=256 -volume.max=20
