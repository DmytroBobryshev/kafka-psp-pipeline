package com.example.psp.apigateway.config;

import java.net.InetSocketAddress;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

/**
 * The {@link KeyResolver} referenced by {@code application.yml}'s {@code
 * spring.cloud.gateway.default-filters}' {@code RequestRateLimiter} (SpEL {@code
 * #{@literal @}ipKeyResolver}). Buckets by the caller's remote IP address - the simplest key that
 * makes sense for this system: there is no authenticated principal at the gateway (ADR-0004's
 * commands enter as plain REST, auth is out of this project's scope), so IP is the only caller
 * identity available. See the module README's "Rate limit" section for the exact
 * replenishRate/burstCapacity and how to trip it.
 */
@Configuration
public class RateLimiterConfig {

    @Bean
    public KeyResolver ipKeyResolver() {
        return exchange -> {
            InetSocketAddress remoteAddress = exchange.getRequest().getRemoteAddress();
            String key =
                    (remoteAddress != null && remoteAddress.getAddress() != null)
                            ? remoteAddress.getAddress().getHostAddress()
                            : "unknown";
            return Mono.just(key);
        };
    }
}
