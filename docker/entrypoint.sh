#!/bin/sh
set -eu

DATA_DIR="${JAVASTREAM_STORAGE_DATA_DIR:-/var/lib/javastream/data}"
LOG_DIR="${JAVASTREAM_LOG_DIR:-/var/lib/javastream/logs}"

mkdir -p "${DATA_DIR}" "${LOG_DIR}"
cd /opt/javastream
ln -sfn "${LOG_DIR}" logs

exec java \
  ${JAVA_OPTS:-} \
  -XX:+UseContainerSupport \
  -Dfile.encoding=UTF-8 \
  -jar /opt/javastream/javastream.jar
