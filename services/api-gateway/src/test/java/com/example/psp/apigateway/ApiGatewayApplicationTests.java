package com.example.psp.apigateway;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Context-load smoke test, same pattern as every other service's {@code XxxApplicationTests}.
 * Runs with no active Spring profile - {@code config.KnownProfileGuard} is disabled via {@code
 * src/test/resources/application.yml} for exactly this reason (it would otherwise refuse to
 * start, correctly, per ADR-0009 - see that class's javadoc). No route requires a live
 * Eureka/Redis to resolve the context (the test-classpath route table is empty), so this proves
 * the WebFlux application itself - filters, actuator, tracing autoconfiguration - wires up
 * cleanly, independent of any running infra. Routing/rate-limiting/circuit-breaker behavior is
 * verified against the real, running stack instead - see this module's README "Prove it".
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ApiGatewayApplicationTests {

    @Test
    void contextLoads() {
        // Intentionally empty: a failing ApplicationContext fails this test.
    }
}
