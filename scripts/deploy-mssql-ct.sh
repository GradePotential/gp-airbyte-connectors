#!/usr/bin/env bash
# Build and deploy source-mssql-ct to the Airbyte VM.
# Usage: ./scripts/deploy-mssql-ct.sh <version>   e.g. 1.0.9
#
# Requires deploy.env in the project root (copy from deploy.env.example).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

VERSION="${1:?Usage: $0 <version>}"

# Load deploy config
ENV_FILE="$ROOT_DIR/deploy.env"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: $ENV_FILE not found. Copy deploy.env.example to deploy.env and fill in your values."
  exit 1
fi
# shellcheck source=/dev/null
source "$ENV_FILE"

IMAGE="${GCP_REGISTRY_REGION}-docker.pkg.dev/${GCP_PROJECT}/${GCP_REGISTRY_REPO}/source-mssql-ct:${VERSION}"
VM_SSH="${AIRBYTE_VM_USER}@${AIRBYTE_VM_IP}"

echo "==> Building JAR (skip tests)"
export PATH="/opt/homebrew/opt/openjdk@21/bin:$PATH"
cd "$ROOT_DIR"
./gradlew :sources:source-mssql-ct:assemble -x test

echo "==> Building Docker image (linux/amd64)"
docker build \
  -f sources/source-mssql-ct/Dockerfile.amd64 \
  --platform linux/amd64 \
  -t "$IMAGE" \
  sources/source-mssql-ct/

echo "==> Transferring image to Airbyte VM"
docker save "$IMAGE" | ssh -i "$AIRBYTE_SSH_KEY" "$VM_SSH" \
  "docker exec -i ${AIRBYTE_K8S_CONTROL_PLANE} ctr -n k8s.io images import -"

echo "==> Updating actor_definition_version in Airbyte DB"
ssh -i "$AIRBYTE_SSH_KEY" "$VM_SSH" \
  "docker exec ${AIRBYTE_K8S_CONTROL_PLANE} \
   kubectl exec -n ${AIRBYTE_K8S_NAMESPACE} ${AIRBYTE_DB_POD} -- \
   psql -U airbyte -d db-airbyte -c \
   \"UPDATE actor_definition_version SET docker_image_tag='${VERSION}' WHERE id='${MSSQL_CT_ACTOR_DEF_VERSION_ID}';\""

echo "==> Done. source-mssql-ct ${VERSION} deployed."
