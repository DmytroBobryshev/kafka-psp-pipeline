package com.example.psp.apigateway.config;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * ADR-0009's "fail fast on startup if the active profile is unexpected" mitigation, made real
 * rather than aspirational. api-gateway has exactly two supported route tables - {@code
 * docker-compose} (Eureka-resolved {@code lb://} URIs) and {@code k8s} (direct Kubernetes Service
 * DNS names) - and picking neither leaves the gateway with ZERO routes (every route is defined
 * inside one profile's YAML, never the profile-agnostic {@code application.yml} - see that file's
 * top comment for why), which fails silently as 404s on every path rather than a clear startup
 * error. This guard turns that into an immediate, readable failure instead.
 *
 * <p>{@code api-gateway.startup.require-known-profile} defaults to {@code true}; the
 * test-classpath {@code application.yml} flips it to {@code false} so {@code
 * ApiGatewayApplicationTests} can load the context with no active profile, the same convention
 * every other service's context-load test already uses.
 */
@Component
@ConditionalOnProperty(
        name = "api-gateway.startup.require-known-profile",
        havingValue = "true",
        matchIfMissing = true)
public class KnownProfileGuard {

    private static final Set<String> KNOWN_PROFILES = Set.of("docker-compose", "k8s");

    public KnownProfileGuard(Environment environment) {
        List<String> active = Arrays.asList(environment.getActiveProfiles());
        boolean knownProfileActive = active.stream().anyMatch(KNOWN_PROFILES::contains);
        if (!knownProfileActive) {
            throw new IllegalStateException(
                    "api-gateway must run under exactly one of the 'docker-compose' or 'k8s' Spring"
                            + " profiles (ADR-0009) - each carries a DIFFERENT route table (Eureka-resolved"
                            + " lb:// vs. direct Kubernetes Service DNS) defined only in that profile's YAML,"
                            + " and picking neither means every route silently disappears instead of a clear"
                            + " error. Active profiles were: "
                            + active);
        }
    }
}
