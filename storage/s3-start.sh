#!/bin/sh
# Starts the S3 gateway with one identity per service.
# The identity file is generated from environment variables at start-up,
# so no access key is ever written into the repository.
set -eu

for v in S3_ADMIN_ACCESS_KEY S3_ADMIN_SECRET_KEY S3_INGEST_ACCESS_KEY \
         S3_INGEST_SECRET_KEY S3_SPARK_ACCESS_KEY S3_SPARK_SECRET_KEY \
         S3_APP_ACCESS_KEY S3_APP_SECRET_KEY; do
  eval "val=\${$v:-}"
  if [ -z "$val" ] || [ "$val" = "change-me" ]; then
    echo "ERROR: $v is not set in .env" >&2
    exit 1
  fi
done

umask 077
cat > /tmp/s3.json <<JSON
{
  "identities": [
    {
      "name": "admin",
      "credentials": [{"accessKey": "${S3_ADMIN_ACCESS_KEY}", "secretKey": "${S3_ADMIN_SECRET_KEY}"}],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    },
    {
      "name": "ingest",
      "credentials": [{"accessKey": "${S3_INGEST_ACCESS_KEY}", "secretKey": "${S3_INGEST_SECRET_KEY}"}],
      "actions": ["Read:raw", "Write:raw", "List:raw",
                  "Read:pipeline-state", "Write:pipeline-state", "List:pipeline-state"]
    },
    {
      "name": "spark",
      "credentials": [{"accessKey": "${S3_SPARK_ACCESS_KEY}", "secretKey": "${S3_SPARK_SECRET_KEY}"}],
      "actions": ["Read:raw", "List:raw",
                  "Read:processed", "Write:processed", "List:processed",
                  "Read:quarantine", "Write:quarantine", "List:quarantine",
                  "Read:published", "Write:published", "List:published",
                  "Read:pipeline-state", "Write:pipeline-state", "List:pipeline-state"]
    },
    {
      "name": "forecast-app",
      "credentials": [{"accessKey": "${S3_APP_ACCESS_KEY}", "secretKey": "${S3_APP_SECRET_KEY}"}],
      "actions": ["Read:published", "List:published"]
    }
  ]
}
JSON

exec weed s3 -filer=filer:8888 -ip.bind=0.0.0.0 -port=8333 -config=/tmp/s3.json
