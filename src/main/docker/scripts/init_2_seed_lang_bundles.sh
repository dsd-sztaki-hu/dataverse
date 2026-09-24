#!/bin/bash
################################################################################
# Seed Hunverse language packs onto the persistent /dv volume.
#
# Packs are baked at ${HOME_DIR}/langBundles. STORAGE_DIR (/dv) is a VOLUME, so
# they must be copied at start. cp -n copies only missing files so CEDAR UUID
# bundles and packs already updated from GitHub are not overwritten.
################################################################################

set -euo pipefail

SRC="${HOME_DIR}/langBundles"
DST="${STORAGE_DIR}/langBundles"

mkdir -p "${DST}"

if [ ! -d "${SRC}" ]; then
  echo "INFO: No language packs at ${SRC}; created empty ${DST}."
  return 0 2>/dev/null || exit 0
fi

cp -n "${SRC}"/*.properties "${DST}/" 2>/dev/null || true
echo "INFO: Seeded missing language packs from ${SRC} to ${DST}."
