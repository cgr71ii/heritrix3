#!/bin/bash

DIR="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
HERITRIX_BUILD_LOG_PREFIX="$1"

if [[ -z "$HERITRIX_BUILD_LOG_PREFIX" ]]; then
  >&2 echo "Syntax: $(basename $0) <build-log-prefix>"
  exit 1
fi

pushd "$DIR" > /dev/null

echo "Creating package..."

mvn --settings .github/workflows/m2-settings.xml -B clean package --file pom.xml \
  > "${HERITRIX_BUILD_LOG_PREFIX}.out" 2> "${HERITRIX_BUILD_LOG_PREFIX}.err"

HERITRIX_BUILD_STATUS="$?"

if [[ "$HERITRIX_BUILD_STATUS" != "0" ]]; then
  >&2 echo "Build didn't finish successfully: $HERITRIX_BUILD_STATUS"
  exit 1
fi

HERITRIX_DIST_PKG_PATTERN="${DIR}/dist/target/heritrix-*-dist.tar.gz"

if [[ "$(ls $HERITRIX_DIST_PKG_PATTERN 2> /dev/null | wc -l)" != "1" ]]; then
  >&2 echo "Couldn't find the dist pkg: $HERITRIX_DIST_PKG_PATTERN"
  exit 1
fi

HERITRIX_DIST_PKG=$(ls $HERITRIX_DIST_PKG_PATTERN)

echo "Package: $HERITRIX_DIST_PKG"

TIMESTAMP=$(date +%s)
HERITRIX_PREFIX="${DIR}/build_${TIMESTAMP}"

while [[ -d "$HERITRIX_PREFIX" ]]; do
  # Update prefix
  sleep 1

  TIMESTAMP=$(date +%s)
  HERITRIX_PREFIX="${DIR}/build_${TIMESTAMP}"
done

# Install

echo "Installing..."

mkdir -p "$HERITRIX_PREFIX"
tar -xzf "$HERITRIX_DIST_PKG" -C "$HERITRIX_PREFIX"

echo "Package path: $HERITRIX_PREFIX"
echo "Finished!"

popd > /dev/null
