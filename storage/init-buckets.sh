#!/bin/sh
# One-off setup: creates the buckets if they do not exist yet.
# Safe to run again; existing buckets and their data are left untouched.
set -eu

BUCKETS="raw processed quarantine published pipeline-state"
SHELL_ARGS="-master=master:9333 -filer=filer:8888"

existing=$(echo "s3.bucket.list" | weed shell $SHELL_ARGS 2>/dev/null || true)

for b in $BUCKETS; do
  if echo "$existing" | grep -qw "$b"; then
    echo "bucket $b already exists"
  else
    echo "creating bucket $b"
    echo "s3.bucket.create -name $b" | weed shell $SHELL_ARGS
  fi
done

echo "Buckets now present:"
echo "s3.bucket.list" | weed shell $SHELL_ARGS
