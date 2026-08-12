# M18 phase 2 - the Kafka Connect worker image.
#
# BASE: Strimzi's own Kafka image, not debezium/connect (which is what infra/compose uses). A
# KafkaConnect pod is started by Strimzi's entrypoint (/opt/kafka/kafka_connect_run.sh) against a
# config file the operator generates, so the image has to have Strimzi's layout. Overriding
# spec.image on a KafkaConnect CR means "same Strimzi image, extra plugins", not "any Connect
# image you like". The tag must match the operator's version pair for Kafka 4.3.0 exactly -
# `kubectl get deploy strimzi-cluster-operator -n strimzi-system -o json | ... STRIMZI_KAFKA_CONNECT_IMAGES`
# is the authority.
#
# WHY THIS IS A DOCKERFILE AND NOT `KafkaConnect.spec.build`
# Strimzi can build this image itself: `spec.build` runs Kaniko in-cluster, downloads the
# artifacts and pushes the result. It needs somewhere to push to - `output.type: docker` requires
# a registry the kubelet can then pull from. This cluster has no registry, by the same decision
# that makes every application image arrive via `kind load docker-image`, and standing one up
# (a registry container, containerd config patched on all three nodes, the nodes restarted) is
# more moving parts than the thing it would replace. With a registry, the equivalent is:
#
#   spec:
#     build:
#       output: { type: docker, image: <registry>/psp-kafka-connect:<tag> }
#       plugins:
#         - name: debezium-postgres
#           artifacts:
#             - type: tgz
#               url: https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/...
#         - name: mongodb
#           artifacts: [{ type: jar, url: https://repo1.maven.org/maven2/org/mongodb/kafka/... }]
#
# and Strimzi rebuilds and rolls the workers when the list changes - genuinely better, and the
# thing to switch to the day this runs anywhere with a registry. Strimzi 1.1 also adds
# `spec.plugins`, which mounts plugins from OCI artifacts as image volumes; that is newer still
# and depends on the ImageVolume feature being enabled on the node.
#
# THE BUILD CONTEXT IS STAGED, NOT CHECKED IN. infra/k8s/scripts/build-images.sh assembles
# ./build/ from two sources before calling `docker build`:
#   * Debezium's Postgres connector, downloaded from Maven Central (cached in ./build/.cache).
#   * three plugin directories COPIED from infra/compose/connect/plugins - the Confluent Avro
#     converter (Confluent's artifacts are not on Maven Central), the MongoDB sink, and this
#     repo's own ByteBufferToBytes SMT, which exists nowhere but that directory. Reused rather
#     than duplicated into infra/k8s/: the same jars, proven against the same connectors, and
#     duplicating 30 MB of jars into a second directory to avoid a relative path is not a win.
# ./build/ is gitignored.
FROM quay.io/strimzi/kafka:1.1.0-kafka-4.3.0

USER root:root

# Strimzi's generated worker config sets plugin.path=/opt/kafka/plugins, and Connect scans one
# level down: each immediate subdirectory is an isolated plugin classloader. Flattening these into
# one directory is the classic way to get Debezium's Avro/Jackson versions fighting the
# converter's.
COPY ./build/plugins/ /opt/kafka/plugins/

USER 1001
