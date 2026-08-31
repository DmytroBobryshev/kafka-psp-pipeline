package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.PaymentStatus;
import com.example.psp.paymentapi.domain.model.PaymentStatusHistoryEntry;
import com.example.psp.paymentapi.domain.port.PaymentRepository;
import com.example.psp.paymentapi.domain.port.PaymentStatusHistoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * M19: the use case behind {@code adapters.in.kafka.PaymentStatusChangedListener} - payment-api's
 * first consumer of {@code payments.payment-status-changed.v1}, closing the gap the module's
 * README has documented since M3: this table has sat frozen in {@code CREATED} for every payment
 * ever created, because nothing downstream of psp-connector/ledger ever told it otherwise. The
 * transactions panel (M19) needs a real status, so this listens for the same event the ledger and
 * webhook-notifier already consume and applies it here too - a THIRD, independent local
 * projection of the same fact, not a new source of truth (ADR-0005: each service keeps its own
 * copy of the parts of the wider truth it needs to answer its own queries).
 *
 * <p>M20 gave this use case two jobs instead of one, run per received event:
 *
 * <ul>
 *   <li>apply the status to {@code payments.status} - {@code PENDING} through the NO-DOWNGRADE
 *       conditional UPDATE ({@link PaymentRepository#applyPendingStatus}), everything else
 *       (SUCCEEDED/FAILED) through the same absolute-value UPDATE ({@link
 *       PaymentRepository#updateStatus}) this use case has always used - see that method's
 *       javadoc for why an ABSOLUTE value is what makes redelivery of a terminal outcome safe;
 *   <li>append one row to the {@code payment_status_history} trail ({@link
 *       PaymentStatusHistoryRepository#tryRecord}) - every received event gets a row, terminal or
 *       not, deduplicated on {@code eventId} by the table's own UNIQUE constraint (V9), never by a
 *       check-then-act read here.
 * </ul>
 *
 * <p>Both writes happen for every call, including a call this method's own guard/dedup logic ends
 * up treating as a no-op (a downgraded PENDING, a duplicate eventId) - there is no "did the status
 * actually change" branch here on purpose: each write is independently idempotent by construction
 * (a conditional UPDATE whose WHERE clause simply matches zero rows; an INSERT whose UNIQUE
 * constraint simply rejects it), so this use case doesn't need to first ask a question whose
 * answer the repository layer already enforces.
 *
 * <p>{@code @Transactional} covers both writes as one unit for the same structural reason as
 * {@code CreatePaymentUseCase}: {@link PaymentRepository#updateStatus}/{@code applyPendingStatus}
 * are {@code @Modifying} JPA queries, and Spring Data JPA requires an active transaction around
 * one. {@link PaymentStatusHistoryRepository#tryRecord} runs its own self-contained
 * {@code saveAndFlush} (see that adapter's javadoc) regardless, so wrapping it in this same
 * transaction changes nothing about ITS atomicity - it is included here only so the status UPDATE
 * and the history INSERT commit or roll back together as one visible unit of work.
 */
@Service
public class ApplyPaymentOutcomeUseCase {

    private static final Logger log = LoggerFactory.getLogger(ApplyPaymentOutcomeUseCase.class);

    private final PaymentRepository paymentRepository;
    private final PaymentStatusHistoryRepository historyRepository;

    public ApplyPaymentOutcomeUseCase(
            PaymentRepository paymentRepository, PaymentStatusHistoryRepository historyRepository) {
        this.paymentRepository = paymentRepository;
        this.historyRepository = historyRepository;
    }

    @Transactional
    public void execute(ApplyPaymentOutcomeCommand command) {
        log.info(
                "Applying payment outcome paymentId={} status={} eventId={}",
                command.paymentId(),
                command.status(),
                command.eventId());

        if (command.status() == PaymentStatus.PENDING) {
            paymentRepository.applyPendingStatus(command.paymentId());
        } else {
            paymentRepository.updateStatus(command.paymentId(), command.status());
        }

        historyRepository.tryRecord(
                PaymentStatusHistoryEntry.record(
                        command.paymentId(), command.status(), command.eventId(), command.occurredAt()));
    }
}
