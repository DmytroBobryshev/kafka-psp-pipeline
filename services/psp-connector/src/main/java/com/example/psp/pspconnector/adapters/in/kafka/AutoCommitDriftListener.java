package com.example.psp.pspconnector.adapters.in.kafka;

import com.example.psp.pspconnector.application.ProcessPaymentRequestUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * EXPERIMENT-ONLY listener for the README's "duplicates vs loss" drill - active only under the
 * {@code auto-commit-drill} Spring profile (see {@code config.KafkaAutoCommitDriftConfig}'s
 * javadoc for why this needs a second real listener instead of a flag on the production one).
 *
 * <p>Note what's ABSENT compared to {@link PaymentRequestedListener}: no {@code Acknowledgment}
 * parameter, no {@code ack.acknowledge()} call. With {@code enable.auto.commit=true}, the Kafka
 * client commits the offset of the last record returned by {@code poll()} on its own timer,
 * whether or not {@link ProcessPaymentRequestUseCase#execute} has actually finished for that
 * record yet - see the README for the real, measured consequence of that gap.
 */
@Component
@Profile("auto-commit-drill")
public class AutoCommitDriftListener {

    private static final Logger log = LoggerFactory.getLogger(AutoCommitDriftListener.class);

    private final ProcessPaymentRequestUseCase useCase;
    private final PaymentRequestedMapper mapper;

    public AutoCommitDriftListener(ProcessPaymentRequestUseCase useCase, PaymentRequestedMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @KafkaListener(
            topics = "${psp-connector.kafka.payment-requested-topic}",
            containerFactory = "autoCommitDriftKafkaListenerContainerFactory")
    public void onMessage(PaymentRequestedEvent event) {
        log.info(
                "[auto-commit-drill] Consumed payment-requested paymentId={} merchantId={}",
                event.paymentId(),
                event.merchantId());
        useCase.execute(mapper.toCommand(event));
    }
}
