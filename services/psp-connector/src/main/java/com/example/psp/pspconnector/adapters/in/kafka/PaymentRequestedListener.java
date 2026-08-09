package com.example.psp.pspconnector.adapters.in.kafka;

import com.example.psp.pspconnector.application.ProcessPaymentRequestUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * M4's first consumer: listens on {@code payments.payment-requested.v1} using the container
 * factory built in {@code config.KafkaConsumerConfig} (manual ack, explicit error handling).
 *
 * <p>Deliberately thin and exception-agnostic: on success it acks; on failure it does nothing
 * special at all - it simply doesn't reach the {@code ack.acknowledge()} line, because the
 * exception thrown by {@link ProcessPaymentRequestUseCase#execute} propagates straight past it.
 * Spring Kafka's container catches that exception and hands it to the
 * {@code CommonErrorHandler} configured on the listener container factory, which decides
 * retry-vs-give-up (ADR-0006 category A) without this class knowing or caring.
 *
 * <p>{@code @Profile("!auto-commit-drill")}: excluded when the README's auto-commit-vs-manual-ack
 * drill is running ({@code adapters.in.kafka.AutoCommitDriftListener} takes over instead) so the
 * two commit strategies are never both active in the same process at once - that would mix two
 * independent members into one consumer group under different commit semantics, muddying the
 * exact comparison the drill exists to make clean.
 */
@Component
@Profile("!auto-commit-drill")
public class PaymentRequestedListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentRequestedListener.class);

    private final ProcessPaymentRequestUseCase useCase;
    private final PaymentRequestedMapper mapper;

    public PaymentRequestedListener(ProcessPaymentRequestUseCase useCase, PaymentRequestedMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @KafkaListener(
            topics = "${psp-connector.kafka.payment-requested-topic}",
            containerFactory = "paymentRequestedKafkaListenerContainerFactory")
    public void onMessage(PaymentRequestedEvent event, Acknowledgment ack) {
        log.info(
                "Consumed payment-requested paymentId={} merchantId={}",
                event.paymentId(),
                event.merchantId());

        useCase.execute(mapper.toCommand(event));

        // Reached only when execute() returns normally (APPROVED or DECLINED, both published).
        // A TIMEOUT never gets here - see ProcessPaymentRequestUseCase.
        ack.acknowledge();
    }
}
