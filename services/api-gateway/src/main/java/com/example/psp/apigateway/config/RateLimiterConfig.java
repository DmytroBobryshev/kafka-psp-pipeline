package com.example.psp.apigateway.config;

import java.net.InetSocketAddress;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

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
