package com.example.psp.discoveryserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;

/**
 * M16 (ADR-0009): a Netflix Eureka registry, and nothing else. api-gateway resolves {@code
 * lb://payment-api} (and the other five services) through it in the {@code docker-compose}
 * profile only.
 *
 * <p>This application has exactly one valid way to run - {@code SPRING_PROFILES_ACTIVE=
 * docker-compose} - and {@link com.example.psp.discoveryserver.config.ComposeProfileGuard} fails
 * the boot process fast, with a clear message, if that profile isn't active. There is
 * deliberately no {@code k8s} counterpart configuration anywhere in this module: per ADR-0009,
 * this service is not deployed to Kubernetes at all (no Deployment, no Service, no Helm chart in
 * M18) because a Kubernetes {@code Service} + its DNS name + kube-proxy + readiness probes
 * already ARE the discovery mechanism there - running Eureka alongside them would duplicate
 * state the platform already maintains authoritatively, with a slower failure-detection path
 * (Eureka's default 30s heartbeat / 90s eviction vs. a readiness probe removing an endpoint in
 * seconds). See this module's README for the full argument; the point of building this service
 * at all is understanding exactly why platforms like Kubernetes replaced it.
 */
@SpringBootApplication
@EnableEurekaServer
public class DiscoveryServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DiscoveryServerApplication.class, args);
    }
}
