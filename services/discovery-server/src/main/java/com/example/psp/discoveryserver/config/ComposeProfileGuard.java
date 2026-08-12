package com.example.psp.discoveryserver.config;

import java.util.Arrays;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * ADR-0009's "fail fast on startup if the active profile is unexpected" mitigation, made real
 * rather than aspirational: this service has exactly one supported run mode ({@code
 * docker-compose}) and refuses to finish starting under any other. Without this, a developer who
 * forgets {@code SPRING_PROFILES_ACTIVE=docker-compose} still gets a server bound to :8761 that
 * *looks* fine - the confusing failure mode ADR-0009 calls out happens two hops downstream, in
 * api-gateway or a service's Eureka client, as an {@code UnknownHostException} or a route that
 * silently resolves to nothing.
 *
 * <p>{@code discovery-server.startup.require-compose-profile} defaults to {@code true}; the
 * test-classpath {@code application.yml} flips it to {@code false} so {@code
 * DiscoveryServerApplicationTests} can load the context without setting a profile, the same
 * convention every other service's context-load test already uses (see e.g. payment-api's {@code
 * src/test/resources/application.yml}).
 */
@Component
@ConditionalOnProperty(
        name = "discovery-server.startup.require-compose-profile",
        havingValue = "true",
        matchIfMissing = true)
public class ComposeProfileGuard {

    private static final String REQUIRED_PROFILE = "docker-compose";

    public ComposeProfileGuard(Environment environment) {
        List<String> active = Arrays.asList(environment.getActiveProfiles());
        if (!active.contains(REQUIRED_PROFILE)) {
            throw new IllegalStateException(
                    "discovery-server must be started with SPRING_PROFILES_ACTIVE=docker-compose "
                            + "(ADR-0009). This service exists ONLY for the compose profile and is never "
                            + "deployed under k8s - see this module's README. Active profiles were: "
                            + active);
        }
    }
}
