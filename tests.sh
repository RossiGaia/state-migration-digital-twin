#!/bin/bash
set -euo pipefail

NAMESPACE=default
CR_NAME="cpa-1"

list_pods() {
  kubectl get pods -n "$NAMESPACE" --no-headers -o custom-columns=NAME:.metadata.name | sort
}

wait_for_new_pods() {
  local before_file="$1"
  local timeout_sec="${2:-120}"
  local elapsed=0

  while (( elapsed < timeout_sec )); do
    local after
    after="$(list_pods)"

    local new_pods
    new_pods="$(comm -13 "$before_file" <(printf "%s\n" "$after"))"

    if [[ -n "$new_pods" ]]; then
      printf "%s\n" "$new_pods"
      return 0
    fi

    sleep 2
    elapsed=$((elapsed + 2))
  done

  return 1
}

echo "Starting tests..."
for i in {1..10}; do
  echo "Iteration $i"

  before_file="$(mktemp)"
  list_pods > "$before_file"

  kubectl patch cpa "$CR_NAME" -n "$NAMESPACE" --type merge -p '{"spec": {"migrate": true}}'

  echo "Waiting for new pods..."
  if new_pods="$(wait_for_new_pods "$before_file" 180)"; then
    echo "New pods detected:"
    echo "$new_pods"

    while IFS= read -r p; do
      echo "----- logs for $p -----"
      kubectl logs -n "$NAMESPACE" "$p" --tail=200 || true
    done <<< "$new_pods"
  else
    echo "No new pods detected within timeout"
  fi

  rm -f "$before_file"

  sleep 5
  kubectl patch cpa "$CR_NAME" -n "$NAMESPACE" --type merge -p '{"spec": {"migrate": false}}'
  sleep 60
done

echo "Tests finished!"