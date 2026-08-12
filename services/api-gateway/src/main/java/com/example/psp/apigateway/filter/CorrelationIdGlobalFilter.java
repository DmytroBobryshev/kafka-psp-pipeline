package com.example.psp.apigateway.filter;

import java.util.UUID;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * Generates {@value #HEADER_NAME} if the inbound request doesn't already carry one, forwards it
 * downstream on the proxied request, and echoes it back on the gateway's own response - the same
 * header name and the same job as {@code libs/common-web}'s {@code CorrelationIdFilter}, applied
 * one hop earlier, at the edge, before a request ever reaches a service.
 *
 * <p><b>Why this isn't just common-web's filter, reused.</b> {@code CorrelationIdFilter} extends
 * {@code OncePerRequestFilter} - a Servlet filter - and {@code common-web} pulls in
 * spring-boot-starter-web transitively. Spring Cloud Gateway is reactive (Reactor Netty/WebFlux);
 * putting spring-boot-starter-web on this module's classpath would make Spring Boot's
 * web-application-type auto-detection pick SERVLET and silently disable the gateway's own
 * reactive autoconfiguration - see this module's pom.xml. This class is the WebFlux-native
 * equivalent ({@link GlobalFilter}, not a Servlet {@code Filter}), deliberately kept in lockstep
 * with the same header name and the same "generate if absent, otherwise pass through" semantics
 * so a request that already has an id (set by a caller, or by a upstream proxy) keeps it
 * end-to-end - there is exactly ONE correlation-id, not a gateway one and a service one.
 *
 * <h2>How this relates to the W3C {@code traceparent} header (M15)</h2>
 *
 * These are deliberately two different, non-overlapping mechanisms, not two names for the same
 * thing:
 *
 * <ul>
 *   <li><b>{@code X-Correlation-Id}</b> (this filter) is an opaque, human-assigned string whose
 *       only job is grep-friendly log correlation for one logical request - it means "these log
 *       lines, across however many services, belong to the same request," and it is carried
 *       explicitly in ADR-0002's event envelope so it survives the synchronous-to-asynchronous
 *       handoff at the Kafka boundary, which {@code traceparent} does not need to (Kafka headers
     *   carry their own {@code traceparent}, propagated by Spring Kafka's Observation API - see
 *       infra/compose/README.md's M15 section).
 *   <li><b>{@code traceparent}</b> (W3C Trace Context) is a structured trace-id + span-id + flags
 *       string that Micrometer Tracing/OpenTelemetry generate, propagate, and own completely -
 *       this class never reads, writes, or forwards it. Once micrometer-tracing-bridge-otel is on
 *       the classpath (this module's pom.xml), Spring Boot auto-configures a {@code Tracer}, a
 *       W3C {@code Propagator}, and - because this is a WebFlux app - a span around every request
 *       the gateway proxies automatically, exported to Tempo over OTLP exactly like the six
 *       services already do.
 * </ul>
 *
 * <p>This project deliberately does not invent a third notion of request identity at the
 * gateway: {@code correlationId} answers "which client request was this," {@code traceId}/{@code
 * traceparent} answer "which causally-connected chain of spans." Ordered to run first
 * ({@link Ordered#HIGHEST_PRECEDENCE}) so the id is on the mutated request before any other
 * filter (rate limiting, the circuit breaker, routing) sees it.
 */
@Component
public class CorrelationIdGlobalFilter implements GlobalFilter, Ordered {

    /** Matches {@code com.example.psp.common.web.correlation.CorrelationIdFilter.HEADER_NAME}. */
    public static final String HEADER_NAME = "X-Correlation-Id";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ServerHttpRequest request = exchange.getRequest();
        String correlationId = request.getHeaders().getFirst(HEADER_NAME);
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }

        ServerHttpRequest mutatedRequest =
                request.mutate().header(HEADER_NAME, correlationId).build();
        // Set on the response now (headers, not body) - Gateway hasn't committed the response yet
        // even for a streamed (SSE) body, since only the body is streamed lazily, not the header
        // write.
        exchange.getResponse().getHeaders().set(HEADER_NAME, correlationId);

        return chain.filter(exchange.mutate().request(mutatedRequest).build());
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
