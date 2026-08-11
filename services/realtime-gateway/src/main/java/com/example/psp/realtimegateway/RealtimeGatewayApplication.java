package com.example.psp.realtimegateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * M12 entry point: consumes {@code payments.payment-requested.v1}, {@code
 * payments.payment-status-changed.v1}, and every {@code refunds.*.v1} event, and pushes them to
 * browsers over SSE, filtered by paymentId and/or merchantId. No database - see {@code README.md}
 * for the architecture, the "broadcast problem" (this module's central point,
 * {@code config.KafkaConsumerConfig}), and how to run it against the {@code infra/compose} stack.
 */
@SpringBootApplication
public class RealtimeGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(RealtimeGatewayApplication.class, args);
    }
}
