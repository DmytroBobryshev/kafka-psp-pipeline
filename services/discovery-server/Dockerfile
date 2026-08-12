# M16: discovery-server is the one Spring service in this repo that runs AS A CONTAINER inside
# docker-compose (ADR-0009), instead of on the host like the six pipeline services and
# api-gateway. Eureka is the compose profile's stand-in for the discovery mechanism Kubernetes
# gives you natively, so it belongs to the compose stack's own lifecycle, not the developer's
# host JVM - see README.md "Why this service runs in a container".
#
# This Dockerfile does NOT run Maven - it copies a jar built on the host with the same toolchain
# every other build in this repo already uses, keeping the image tiny and the build fast rather
# than duplicating a Maven build inside Docker:
#
#   mvn -pl services/discovery-server -am package -DskipTests
#   docker compose -f infra/compose/docker-compose.yml build discovery-server
#   docker compose -f infra/compose/docker-compose.yml up -d discovery-server
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY target/discovery-server.jar app.jar
EXPOSE 8761
ENTRYPOINT ["java", "-jar", "app.jar"]
