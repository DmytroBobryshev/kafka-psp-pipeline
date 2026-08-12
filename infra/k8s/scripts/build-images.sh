#!/usr/bin/env bash
#
# infra/k8s/scripts/build-images.sh - build every container image this phase needs and load them
# into the kind cluster.
#
# WHY `kind load` AND NOT `docker push`
# A kind "node" is a Docker container running its own containerd. It does not share the host
# Docker daemon's image store, so an image that `docker images` lists is invisible to the kubelet
# and a pod referencing it fails with ErrImagePull against a registry that was never involved.
# `kind load docker-image` streams the image from the host daemon into each node's containerd.
# There is no registry anywhere in this phase, which is why every chart sets
# imagePullPolicy: IfNotPresent - with the default Always, the kubelet would try to pull
# `psp/payment-api:...` from Docker Hub and fail even though the image is already on the node.
#
# TAGS ARE NOT `latest`
# The default tag is <pom version>-<git short sha>[-dirty.<hash of the jars>]. Two reasons:
#   * `latest` plus imagePullPolicy: IfNotPresent means the node keeps the FIRST image it saw
#     forever - you rebuild, reload, redeploy, and run the old code, with no error anywhere.
#   * A Deployment whose pod template does not change is not rolled. With a real tag,
#     `helm upgrade` changes the image string, the ReplicaSet is replaced, and
#     `kubectl rollout status` is a statement about the new build rather than a no-op.
# The -dirty component hashes the built jars, so an uncommitted change still produces a new tag.
#
# Usage:
#   ./infra/k8s/scripts/build-images.sh                 build + load everything
#   ./infra/k8s/scripts/build-images.sh --skip-maven    reuse the jars already in services/*/target
#   ./infra/k8s/scripts/build-images.sh --print-tag     print the tag and exit (used by deploy.sh)
#   IMAGE_TAG=whatever ./infra/k8s/scripts/build-images.sh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CLUSTER_NAME="${CLUSTER_NAME:-kafka-psp}"
IMAGE_REPO="${IMAGE_REPO:-psp}"
DEBEZIUM_VERSION="${DEBEZIUM_VERSION:-3.6.1.Final}"
STRIMZI_KAFKA_IMAGE="${STRIMZI_KAFKA_IMAGE:-quay.io/strimzi/kafka:1.1.0-kafka-4.3.0}"

SERVICES=(payment-api psp-connector ledger webhook-notifier analytics realtime-gateway api-gateway)

SKIP_MAVEN=0
PRINT_TAG_ONLY=0
for arg in "$@"; do
  case "$arg" in
    --skip-maven) SKIP_MAVEN=1 ;;
    --print-tag)  PRINT_TAG_ONLY=1; SKIP_MAVEN=1 ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done

log() { printf '\n==> %s\n' "$*"; }

# ---------------------------------------------------------------------------------------------
# 1. Build the jars
# ---------------------------------------------------------------------------------------------
if [[ "$SKIP_MAVEN" -eq 0 ]]; then
  log "mvn package (skipping tests - the reactor's own test run is a separate concern)"
  (cd "$REPO_ROOT" && mvn -q -T1C -DskipTests package)
fi

for svc in "${SERVICES[@]}"; do
  if [[ ! -f "$REPO_ROOT/services/$svc/target/$svc.jar" ]]; then
    echo "missing $REPO_ROOT/services/$svc/target/$svc.jar - run without --skip-maven" >&2
    exit 1
  fi
done

# ---------------------------------------------------------------------------------------------
# 2. Work out the tag
# ---------------------------------------------------------------------------------------------
if [[ -z "${IMAGE_TAG:-}" ]]; then
  VERSION="$(cd "$REPO_ROOT" && sed -n 's|^  <version>\(.*\)</version>|\1|p' pom.xml | head -1)"
  GIT_SHA="$(cd "$REPO_ROOT" && git rev-parse --short HEAD 2>/dev/null || echo nogit)"
  TAG="${VERSION}-${GIT_SHA}"
  if ! (cd "$REPO_ROOT" && git diff --quiet HEAD -- services libs pom.xml infra/k8s 2>/dev/null); then
    # Uncommitted changes: hash what actually goes INTO the images so two different working trees
    # cannot share a tag. That means the jars AND the Dockerfiles - a Dockerfile-only change (a
    # different base image, a different USER) produces identical jars, and reusing the tag would
    # leave every node holding an image whose tag no longer describes it.
    CONTENT_HASH="$(
      {
        for svc in "${SERVICES[@]}"; do
          shasum -a 256 "$REPO_ROOT/services/$svc/target/$svc.jar" "$REPO_ROOT/services/$svc/Dockerfile"
        done
        shasum -a 256 "$REPO_ROOT/infra/k8s/connect/Dockerfile"
        echo "debezium=${DEBEZIUM_VERSION} base=${STRIMZI_KAFKA_IMAGE}"
      } | shasum -a 256 | cut -c1-8
    )"
    TAG="${TAG}-dirty.${CONTENT_HASH}"
  fi
  IMAGE_TAG="$TAG"
fi

if [[ "$PRINT_TAG_ONLY" -eq 1 ]]; then
  echo "$IMAGE_TAG"
  exit 0
fi

log "image tag: ${IMAGE_TAG}"

# ---------------------------------------------------------------------------------------------
# 3. Stage the Kafka Connect plugin tree
# ---------------------------------------------------------------------------------------------
CONNECT_DIR="$REPO_ROOT/infra/k8s/connect"
STAGE="$CONNECT_DIR/build"
CACHE="$STAGE/.cache"
COMPOSE_PLUGINS="$REPO_ROOT/infra/compose/connect/plugins"

log "staging Kafka Connect plugins in ${STAGE}"
mkdir -p "$CACHE" "$STAGE/plugins"
rm -rf "$STAGE/plugins"/*

DBZ_TGZ="$CACHE/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz"
if [[ ! -f "$DBZ_TGZ" ]]; then
  echo "    downloading Debezium ${DEBEZIUM_VERSION} from Maven Central"
  curl -fsSL -o "$DBZ_TGZ" \
    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz"
fi
tar -xzf "$DBZ_TGZ" -C "$STAGE/plugins"

# The three directories that come from infra/compose (read-only; nothing here writes to it).
cp -R "$COMPOSE_PLUGINS/mongodb-kafka-connect"        "$STAGE/plugins/"
cp -R "$COMPOSE_PLUGINS/kafka-connect-avro-converter" "$STAGE/plugins/"
mkdir -p "$STAGE/plugins/outbox-bytebuffer-smt"
cp "$COMPOSE_PLUGINS/outbox-bytebuffer-smt/outbox-bytebuffer-smt.jar" "$STAGE/plugins/outbox-bytebuffer-smt/"

echo "    plugin directories: $(ls "$STAGE/plugins" | tr '\n' ' ')"

# ---------------------------------------------------------------------------------------------
# 4. Build the images
# ---------------------------------------------------------------------------------------------
docker image inspect "$STRIMZI_KAFKA_IMAGE" >/dev/null 2>&1 || docker pull "$STRIMZI_KAFKA_IMAGE"

log "docker build kafka-connect"
docker build -q -t "${IMAGE_REPO}/kafka-connect:${IMAGE_TAG}" "$CONNECT_DIR" >/dev/null

for svc in "${SERVICES[@]}"; do
  log "docker build ${svc}"
  docker build -q -t "${IMAGE_REPO}/${svc}:${IMAGE_TAG}" "$REPO_ROOT/services/${svc}" >/dev/null
done

# ---------------------------------------------------------------------------------------------
# 5. Load them into kind
# ---------------------------------------------------------------------------------------------
# One `kind load` call with every image: it archives once and streams to all three nodes, instead
# of re-archiving per image. On a 3-node cluster with eight ~200 MB images that is the difference
# between about a minute and about four.
log "kind load docker-image -> cluster '${CLUSTER_NAME}' (all 3 nodes)"
IMAGES=("${IMAGE_REPO}/kafka-connect:${IMAGE_TAG}")
for svc in "${SERVICES[@]}"; do IMAGES+=("${IMAGE_REPO}/${svc}:${IMAGE_TAG}"); done
kind load docker-image --name "$CLUSTER_NAME" "${IMAGES[@]}"

log "done. Deploy with:"
echo "    helm upgrade --install psp infra/k8s/charts/psp-platform -n kafka --set global.imageTag=${IMAGE_TAG}"
echo
echo "IMAGE_TAG=${IMAGE_TAG}"
