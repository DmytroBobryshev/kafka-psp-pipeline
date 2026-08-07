/**
 * Spring configuration, bean wiring, and profiles (ADR-0007).
 *
 * <p>Empty at M1 by design: there is no datasource, Kafka, or Eureka/discovery config to wire
 * yet, so component scanning + the default auto-configuration from
 * {@code spring-boot-starter-web}/{@code spring-boot-starter-actuator} is enough for the service
 * to start standalone. Expect {@code KafkaConfig}, persistence config, and
 * {@code docker-compose}/{@code k8s} profile beans (ADR-0009) here starting M2/M3.
 */
package com.example.psp.paymentapi.config;
