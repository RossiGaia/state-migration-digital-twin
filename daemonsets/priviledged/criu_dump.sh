#!/bin/bash

# CRIU dump
set -euo pipefail

URL="https://127.0.0.1:10250/checkpoint/${CRIU_DUMP_NAMESPACE}/${CRIU_DUMP_POD_NAME}/${CRIU_DUMP_CONTAINER_NAME}"

if curl -k -X POST \
    -H "Authorization: Bearer ${CRIU_DUMP_TOKEN}" \
    "${URL}"; then
    echo "Dump successful."
else
    echo "Dump failed."
fi

# Image creation with buildah
newcontainer=$(buildah from scratch)
buildah add "$newcontainer" \
    "/var/lib/kubelet/checkpoints/checkpoint-${CRIU_DUMP_POD_NAME}_${CRIU_DUMP_NAMESPACE}-${CRIU_DUMP_CONTAINER_NAME}"-*.tar /
buildah config --annotation="io.kubernetes.cri-o.annotations.checkpoint.name=${CRIU_DUMP_CONTAINER_NAME}" "$newcontainer"
buildah commit "$newcontainer" checkpoint-image:"${CRIU_DUMP_POD_NAME}_${CRIU_DUMP_CONTAINER_NAME}"
buildah rm "$newcontainer"
buildah tag localhost/checkpoint-image:"${CRIU_DUMP_POD_NAME}_${CRIU_DUMP_CONTAINER_NAME}" docker.io/rssgai/checkpoint-image:"${CRIU_DUMP_POD_NAME}_${CRIU_DUMP_CONTAINER_NAME}"
buildah push docker.io/rssgai/checkpoint-image:"${CRIU_DUMP_POD_NAME}_${CRIU_DUMP_CONTAINER_NAME}"