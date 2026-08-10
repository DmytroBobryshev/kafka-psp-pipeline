package com.example.psp.ledger.adapters.in.kafka;

import com.example.psp.ledger.application.RecordLedgerEntryUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * M7's consumer: listens on {@code payments.payment-status-changed.v1} using the transactional
 * container factory built in {@code config.KafkaConsumerConfig}.
 *
 * <h2>Where the Kafka transaction actually begins and ends</h2>
 *
 * <p>There is <b>no {@code Acknowledgment} parameter here</b>, and its absence is the point. In
 * {@code psp-connector} (M4/M5) the listener called {@code ack.acknowledge()} to commit the offset
 * as a separate, independent act. Here the offset commit is not this method's business at all: it
 * is part of a Kafka transaction, and it happens only if this method returns normally.
 *
 * <p>Concretely, for every record, the listener container
 * ({@code KafkaMessageListenerContainer.ListenerConsumer#invokeRecordListenerInTx}) does the
 * following, because {@code ContainerProperties.setKafkaAwareTransactionManager(...)} was set:
 *
 * <pre>
 *   producer = producerFactory.createProducer(prefix)   // first use: producer.initTransactions()
 *   producer.beginTransaction()                          // KafkaTransactionManager.doBegin
 *       &lt;-- this method runs; the publisher's send() joins the open transaction --&gt;
 *   producer.sendOffsetsToTransaction(offsets, consumerGroupMetadata)  // container, before commit
 *   producer.commitTransaction()                         // KafkaTransactionManager.doCommit
 * </pre>
 *
 * and, if this method throws:
 *
 * <pre>
 *   producer.abortTransaction()                          // KafkaTransactionManager.doRollback
 * </pre>
 *
 * <p>Spring calls those four {@code org.apache.kafka.clients.producer.Producer} methods on our
 * behalf; nothing in this service calls them by hand. {@code initTransactions()} is invoked once
 * per producer instance when {@code DefaultKafkaProducerFactory} first creates it - that is the
 * call that registers the {@code transactional.id} with the transaction coordinator and bumps the
 * producer epoch, i.e. the call that fences zombies (see {@code config.KafkaProducerConfig}).
 *
 * <h2>Why {@code @Transactional} is here as well</h2>
 *
 * <p>Strictly speaking it is redundant: the container has already begun the transaction by the time
 * this method is entered, so {@code @Transactional} finds an existing {@code KafkaResourceHolder}
 * bound to the thread and <b>participates</b> in it rather than starting a second one. It is kept
 * because it makes the transactional boundary visible at the call site and because it names the
 * transaction manager explicitly - this service has two ({@code kafkaTransactionManager} and the
 * JPA {@code transactionManager}), and which one wraps what is the entire subject of the module.
 *
 * <p><b>It is not a substitute for the container's transaction manager.</b> Remove
 * {@code setKafkaAwareTransactionManager} from the container factory and this annotation alone
 * would still open a Kafka transaction around the produce - but the consumed offsets would be
 * committed separately, outside it, by the container's normal ack path. Output and offsets would
 * no longer move together, and the service would be silently back to at-least-once with no
 * compile-time or runtime complaint. Only the container can add the offsets, because only the
 * container owns the consumer.
 *
 * <h2>Failure behaviour</h2>
 *
 * <p>Deliberately exception-agnostic, like {@code psp-connector}'s listener: any exception from
 * {@link RecordLedgerEntryUseCase#execute} propagates out of this method, which aborts the Kafka
 * transaction, after which the container's {@code AfterRollbackProcessor}
 * ({@code config.KafkaConsumerConfig}) decides whether to seek back and redeliver.
 */
@Component
public class PaymentStatusChangedListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentStatusChangedListener.class);

    private final RecordLedgerEntryUseCase useCase;
    private final PaymentStatusChangedMapper mapper;

    public PaymentStatusChangedListener(
            RecordLedgerEntryUseCase useCase, PaymentStatusChangedMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @KafkaListener(
            topics = "${ledger.kafka.payment-status-changed-topic}",
            containerFactory = "paymentStatusChangedKafkaListenerContainerFactory")
    @Transactional("kafkaTransactionManager")
    public void onMessage(PaymentStatusChangedEvent event) {
        log.info(
                "Consumed payment-status-changed eventId={} paymentId={} merchantId={} status={}",
                event.envelope().eventId(),
                event.paymentId(),
                event.merchantId(),
                event.status());

        useCase.execute(mapper.toCommand(event));

        // No ack.acknowledge() here on purpose - see this class's javadoc. Returning normally is
        // what commits both the produced ledger entry and the consumed offset, together.
    }
}
