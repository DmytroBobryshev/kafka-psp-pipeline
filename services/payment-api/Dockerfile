# M18 phase 2 - container image for payment-api: REST edge: takes the payment command over HTTP, writes the outbox row Debezium relays (M6).
#
# Same pattern as services/discovery-server/Dockerfile, which was the first image in this repo:
# this Dockerfile does NOT run Maven. It copies a jar built on the host by the same reactor
# every other build here already uses, which keeps the image small and the build fast instead of
# duplicating a Maven build (and a ~1 GB dependency download) inside Docker. Build it with
# infra/k8s/scripts/build-images.sh, which runs the Maven build first and then loads the result
# into kind - a kind node cannot pull from the host Docker daemon, so `kind load docker-image`
# is not an optimisation here, it is the only delivery mechanism (there is no registry).
#
#   mvn -pl services/payment-api -am package -DskipTests
#   docker build -t psp/payment-api:<tag> services/payment-api
#   kind load docker-image psp/payment-api:<tag> --name kafka-psp
FROM eclipse-temurin:21-jre-alpine

# Run as a non-root user. kind's containerd honours this, and it is the difference between a
# compromised Spring service being able to write to the image filesystem and not.
RUN addgroup -g 1001 -S psp && adduser -u 1001 -S -G psp psp

WORKDIR /app
COPY target/payment-api.jar app.jar
RUN chown -R psp:psp /app
# NUMERIC, not `USER psp`. The pod sets runAsNonRoot: true, and the kubelet can only verify that
# from a numeric uid - a named user gives
# "container has runAsNonRoot and image has non-numeric user (psp), cannot verify user is non-root"
# and the container never starts.
USER 1001

# The pod sets memory LIMITS (see the chart's values.yaml); MaxRAMPercentage makes the JVM read
# the cgroup limit rather than the node's total memory, which on a 3-node kind cluster inside one
# Docker VM is the difference between a 300 MB heap and a 6 GB one. Explicit rather than
# inherited so "why was it OOMKilled" is answerable from this file.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=70 -XX:+ExitOnOutOfMemoryError -Djava.security.egd=file:/dev/./urandom"

EXPOSE 8085
ENTRYPOINT ["java", "-jar", "app.jar"]
