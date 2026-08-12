# M18 phase 2 - container image for analytics: Kafka Streams windowed aggregation (M10), MongoDB projections.
#
# Same pattern as services/discovery-server/Dockerfile, which was the first image in this repo:
# this Dockerfile does NOT run Maven. It copies a jar built on the host by the same reactor
# every other build here already uses, which keeps the image small and the build fast instead of
# duplicating a Maven build (and a ~1 GB dependency download) inside Docker. Build it with
# infra/k8s/scripts/build-images.sh, which runs the Maven build first and then loads the result
# into kind - a kind node cannot pull from the host Docker daemon, so `kind load docker-image`
# is not an optimisation here, it is the only delivery mechanism (there is no registry).
#
#   mvn -pl services/analytics -am package -DskipTests
#   docker build -t psp/analytics:<tag> services/analytics
#   kind load docker-image psp/analytics:<tag> --name kafka-psp
#
# NOT eclipse-temurin:21-jre-alpine, unlike every other service's Dockerfile in this repo. M10's
# Kafka Streams topology opens RocksDB state stores, and org.rocksdb:rocksdbjni ships exactly one
# Linux native library, built against glibc. Alpine's C library is musl, which does not provide a
# glibc-compatible libstdc++.so.6 under any package - the failure is not an ABI mismatch you can
# paper over with `apk add libstdc++`, it is a library that plain does not exist on musl, and the
# process dies with "Exception in thread ...-GlobalStreamThread ... UnsatisfiedLinkError: Error
# loading shared library libstdc++.so.6" the moment the first state store initialises. jre-jammy
# (Ubuntu, glibc) is the smallest Temurin base that satisfies it. Every other service here has no
# RocksDB dependency and stays on Alpine for the smaller image.
FROM eclipse-temurin:21-jre-jammy

# libstdc++6 for RocksDB's JNI library - see the base-image comment above. Ubuntu's JRE base
# usually pulls it in transitively, but that is an accident of what else is installed, not a
# guarantee, so it is named explicitly here rather than relied upon.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# Run as a non-root user. kind's containerd honours this, and it is the difference between a
# compromised Spring service being able to write to the image filesystem and not.
RUN groupadd -g 1001 psp && useradd -u 1001 -g psp -M -s /usr/sbin/nologin psp

WORKDIR /app
COPY target/analytics.jar app.jar
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

EXPOSE 8089
ENTRYPOINT ["java", "-jar", "app.jar"]
