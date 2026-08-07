package com.example.psp.paymentapi.adapters.out.kafka;

import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.port.PaymentEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * M1 stub Kafka adapter: logs instead of publishing. No {@code spring-kafka} dependency is even
 * on this module's classpath yet - the parent POM already manages {@code spring-kafka}'s version
 * centrally (via the imported Spring Boot BOM) so M3 only needs to add the dependency, not decide
 * its version.
 *
 * <p>Real production of {@code payments.requested} (key/value serialization, headers, the
 * envelope from {@code libs/common-events}) lands in M3.
 */
@Component
public class LoggingPaymentEventPublisher implements PaymentEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(LoggingPaymentEventPublisher.class);

    @Override
    public void publishPaymentCreated(Payment payment) {
        log.info(
                "[stub] would publish payments.requested for paymentId={} merchantId={} status={}",
                payment.getId(),
                payment.getMerchantId(),
                payment.getStatus());
    }
}
