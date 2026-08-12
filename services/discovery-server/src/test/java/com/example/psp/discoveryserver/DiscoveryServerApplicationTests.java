package com.example.psp.discoveryserver;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Context-load smoke test, same pattern as every other service's {@code XxxApplicationTests}.
 * Runs with no active Spring profile - {@code config.ComposeProfileGuard} is disabled via {@code
 * src/test/resources/application.yml} for exactly this reason (it would otherwise refuse to
 * start, correctly, per ADR-0009 - see that class's javadoc).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class DiscoveryServerApplicationTests {

    @Test
    void contextLoads() {
        // Intentionally empty: a failing ApplicationContext fails this test.
    }
}
