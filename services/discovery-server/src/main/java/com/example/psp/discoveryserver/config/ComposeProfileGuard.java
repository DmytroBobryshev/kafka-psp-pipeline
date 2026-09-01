package com.example.psp.discoveryserver.config;

import java.util.Arrays;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

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
