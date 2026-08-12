package com.example.psp.apigateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * M16 (ADR-0004, ADR-0009): the single REST entry point. All external traffic - the React UI and
 * merchant API clients - enters here over HTTP; the gateway owns CORS, rate limiting, the
 * circuit breaker, and correlation-id injection. Routes to the six pipeline services live in
 * profile-specific YAML ({@code application-docker-compose.yml} uses Eureka-resolved {@code
 * lb://} URIs, {@code application-k8s.yml} uses direct Kubernetes Service DNS names) - see
 * {@link com.example.psp.apigateway.config.KnownProfileGuard} for why exactly one of those two
 * profiles must be active, and this module's README for the full route table and the
 * Eureka-vs-k8s-discovery argument.
 *
 * <p>No {@code @EnableEurekaClient}/{@code @EnableDiscoveryClient} needed here:
 * spring-cloud-starter-netflix-eureka-client's autoconfiguration activates on its own once
 * {@code eureka.client.enabled=true} (the docker-compose profile only), which is also what makes
 * {@code lb://} URIs resolvable via Spring Cloud LoadBalancer.
 */
@SpringBootApplication
public class ApiGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApiGatewayApplication.class, args);
    }
}
