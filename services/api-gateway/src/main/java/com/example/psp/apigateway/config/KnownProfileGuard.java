package com.example.psp.apigateway.config;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

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
