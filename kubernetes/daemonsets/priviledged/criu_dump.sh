#!/bin/bash

# CRIU dump
set -euo pipefail

URL="https://127.0.0.1:10250/checkpoint/${CRIU_DUMP_NAMESPACE}/${CRIU_DUMP_POD_NAME}/${CRIU_DUMP_CONTAINER_NAME}"

RESP="$(curl -fsSk -X POST -H "Authorization: Bearer ${CRIU_DUMP_TOKEN}" "${URL}")"

TAR_PATH="$(echo ${RESP} | jq -r '.items[0] // empty')"

# Image creation with buildah
newcontainer=$(buildah from scratch)
buildah add "$newcontainer" "${TAR_PATH}" /
buildah config --annotation="io.kubernetes.cri-o.annotations.checkpoint.name=${CRIU_DUMP_CONTAINER_NAME}" "$newcontainer"
buildah commit "$newcontainer" checkpoint-image:"${CRIU_DUMP_POD_NAME}_${CRIU_DUMP_CONTAINER_NAME}"
buildah rm "$newcontainer"
buildah tag localhost/checkpoint-image:"${CRIU_DUMP_POD_NAME}_${CRIU_DUMP_CONTAINER_NAME}" docker.io/rssgai/checkpoint-image:"${CRIU_DUMP_POD_NAME}_${CRIU_DUMP_CONTAINER_NAME}"
buildah push docker.io/rssgai/checkpoint-image:"${CRIU_DUMP_POD_NAME}_${CRIU_DUMP_CONTAINER_NAME}"
