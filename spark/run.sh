#!/bin/sh
# Entry point for the PySpark container.
#   run.sh validate --month 2014-04 [--force]
#   run.sh validate --all
#   run.sh release --quarter 2014-Q2 [--force] [--fail-before-manifest]
#   run.sh release --all
#   run.sh test
set -eu
job="${1:-}"
[ $# -gt 0 ] && shift
case "$job" in
  validate) exec /opt/spark/bin/spark-submit /app/jobs/validate_month.py "$@" ;;
  release)  exec /opt/spark/bin/spark-submit /app/jobs/release_quarter.py "$@" ;;
  test)     /opt/spark/bin/spark-submit /app/tests/test_validate.py
            exec /opt/spark/bin/spark-submit /app/tests/test_release.py ;;
  *)        echo "usage: run.sh {validate|release|test} [options]" >&2; exit 2 ;;
esac
