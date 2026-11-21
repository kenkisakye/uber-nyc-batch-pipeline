#!/bin/sh
# Entry point for the PySpark container.
#   run.sh validate --month 2014-04 [--force]
#   run.sh validate --all
#   run.sh test
set -eu
job="${1:-}"
[ $# -gt 0 ] && shift
case "$job" in
  validate) exec /opt/spark/bin/spark-submit /app/jobs/validate_month.py "$@" ;;
  test)     exec /opt/spark/bin/spark-submit /app/tests/test_validate.py "$@" ;;
  *)        echo "usage: run.sh {validate|test} [options]" >&2; exit 2 ;;
esac
