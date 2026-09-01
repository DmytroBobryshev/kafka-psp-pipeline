package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.model.PaymentHistoryItem;
import com.example.psp.paymentapi.domain.model.PaymentPage;
import com.example.psp.paymentapi.domain.model.PaymentStatus;
import com.example.psp.paymentapi.domain.model.PaymentStatusHistoryEntry;
import com.example.psp.paymentapi.domain.model.Refund;
import com.example.psp.paymentapi.domain.model.RefundHistoryItem;
import com.example.psp.paymentapi.domain.model.RefundStatusHistoryEntry;
import com.example.psp.paymentapi.domain.port.PaymentRepository;
import com.example.psp.paymentapi.domain.port.PaymentStatusHistoryRepository;
import com.example.psp.paymentapi.domain.port.RefundRepository;
import com.example.psp.paymentapi.domain.port.RefundStatusHistoryRepository;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.springframework.stereotype.Service;

/**
 * M19's read side of the transactions panel: search, single-payment lookup, and a payment's
 * refunds - three closely-related queries bundled into one use case rather than three separate
 * classes, the same "one class per cohesive feature, not per verb" shape
 * {@code MerchantConfigUseCase} already established for the write side of a different feature.
 *
 * <p>No {@code @Transactional} anywhere here: every method is a single read against a single
 * repository, and Spring Data's {@code findById}/derived queries/{@code @Query} SELECTs already
 * run in their own implicit, read-only-by-default transaction - there is nothing here for an
 * explicit annotation to make atomic that is not already atomic.
 */
@Service
public class PaymentQueryUseCase {

    // M20: the synthetic CREATED entry's source - see #history's javadoc and
    // domain.model.PaymentHistoryItem's javadoc for why this is a literal, not a stored column.
    private static final String SOURCE_PAYMENT_API = "payment-api";

    // Every payment_status_history row's source: psp-connector is the sole publisher of
    // payments.payment-status-changed.v1 (see db/migration/V9's comment), so there is exactly one
    // possible value and nothing to look up per-row.
    private static final String SOURCE_PSP_CONNECTOR = "psp-connector";

    // M23: refund_status_history's FUNDS_RESERVED rows are the one status this service's own
    // sole-publisher shortcut above does NOT hold for - refunds.funds-reserved.v1 is published by
    // the ledger, not psp-connector, so that one status is looked up per-row instead.
    private static final String SOURCE_LEDGER = "ledger";
    private static final String STATUS_FUNDS_RESERVED = "FUNDS_RESERVED";

    private final PaymentRepository paymentRepository;
    private final RefundRepository refundRepository;
    private final PaymentStatusHistoryRepository historyRepository;
    private final RefundStatusHistoryRepository refundHistoryRepository;

    public PaymentQueryUseCase(
            PaymentRepository paymentRepository,
            RefundRepository refundRepository,
            PaymentStatusHistoryRepository historyRepository,
            RefundStatusHistoryRepository refundHistoryRepository) {
        this.paymentRepository = paymentRepository;
        this.refundRepository = refundRepository;
        this.historyRepository = historyRepository;
        this.refundHistoryRepository = refundHistoryRepository;
    }

    /**
     * @param merchantId filter, or {@code null} to match every merchant.
     * @param status     filter, or {@code null} to match every status.
     * @param page       zero-based page index - already clamped by the web adapter.
     * @param size       page size - already clamped by the web adapter.
     */
    public PaymentPage search(String merchantId, PaymentStatus status, int page, int size) {
        return paymentRepository.search(merchantId, status, page, size);
    }

    /**
     * @throws NoSuchElementException if no payment exists with this id - translated to
     *                                {@code 404} by common-web's {@code GlobalExceptionHandler},
     *                                the same convention {@code ledger}'s
     *                                {@code GetRefundSagaStateUseCase} already uses for the
     *                                identical shape of lookup.
     */
    public Payment getById(UUID paymentId) {
        return paymentRepository
                .findById(paymentId)
                .orElseThrow(() -> new NoSuchElementException("No payment with id=" + paymentId));
    }

    /**
     * Every refund requested against {@code paymentId}, in this service's own local view (M11) -
     * see {@link Refund}'s javadoc for why this is not the ledger's saga state. An unknown
     * {@code paymentId} is indistinguishable from "no refunds yet" here on purpose: unlike
     * {@link #getById}, this is a sub-resource listing, and an empty list is already the correct,
     * unambiguous answer for "no refunds" - a 404 would only be honest if this method first
     * checked the payment itself exists, which is an extra query this endpoint does not need to
     * pay for just to distinguish two cases a caller cannot usefully tell apart anyway.
     */
    public List<Refund> listRefunds(UUID paymentId) {
        return refundRepository.findByPaymentId(paymentId);
    }

    /**
     * M20's status-trail read: {@code GET /api/payments/{id}/history}'s full PENDING -&gt;
     * SUCCEEDED/FAILED view - see {@link PaymentHistoryItem}'s javadoc for the exact assembly
     * this performs (one synthetic {@code CREATED} entry from the payment row, plus every
     * recorded {@code payment_status_history} row, merged and sorted {@code occurredAt}
     * ascending).
     *
     * @throws NoSuchElementException if no payment exists with this id - same 404 convention as
     *                                {@link #getById}, reused directly rather than duplicated:
     *                                a history for a payment that doesn't exist is exactly as
     *                                meaningless as the payment itself not existing.
     */
    public List<PaymentHistoryItem> history(UUID paymentId) {
        Payment payment = getById(paymentId);

        List<PaymentHistoryItem> items = new ArrayList<>();
        items.add(
                new PaymentHistoryItem(
                        PaymentStatus.CREATED.name(), payment.getCreatedAt(), null, SOURCE_PAYMENT_API, null));
        for (PaymentStatusHistoryEntry entry : historyRepository.findByPaymentId(paymentId)) {
            items.add(
                    new PaymentHistoryItem(
                            entry.getStatus(),
                            entry.getOccurredAt(),
                            entry.getEventId(),
                            // EXPIRED events are payment-api's own sweep, not a provider outcome
                            PaymentStatus.EXPIRED.name().equals(entry.getStatus())
                                    ? SOURCE_PAYMENT_API
                                    : SOURCE_PSP_CONNECTOR,
                            entry.getProviderReference()));
        }

        return items.stream().sorted(Comparator.comparing(PaymentHistoryItem::occurredAt)).toList();
    }

    /**
     * M23's status-trail read: {@code GET /api/payments/{paymentId}/refunds/{refundId}/history} -
     * the refund-path mirror of {@link #history}. One synthetic {@code REQUESTED} entry from the
     * {@link Refund} row's own {@code createdAt}, plus every recorded
     * {@code refund_status_history} row, merged and sorted {@code occurredAt} ascending. Every
     * non-REQUESTED entry is attributed by status - {@code FUNDS_RESERVED} to
     * {@code "ledger"} (the sole publisher of {@code refunds.funds-reserved.v1}), every other
     * status to {@code "psp-connector"} - same shortcut {@link #history} takes for
     * {@code payment_status_history}, split one status wider here since this trail has two
     * upstream publishers instead of one.
     *
     * @throws NoSuchElementException if {@code refundId} does not exist or does not belong to
     *                                {@code paymentId} - both answer 404, and are deliberately
     *                                indistinguishable to the caller (see
     *                                {@code domain.port.RefundRepository#findByIdAndPaymentId}'s
     *                                javadoc).
     */
    public List<RefundHistoryItem> refundHistory(UUID paymentId, UUID refundId) {
        Refund refund =
                refundRepository
                        .findByIdAndPaymentId(refundId, paymentId)
                        .orElseThrow(
                                () ->
                                        new NoSuchElementException(
                                                "No refund with id=" + refundId + " for paymentId=" + paymentId));

        List<RefundHistoryItem> items = new ArrayList<>();
        items.add(new RefundHistoryItem("REQUESTED", refund.getCreatedAt(), null, SOURCE_PAYMENT_API, null));
        for (RefundStatusHistoryEntry entry : refundHistoryRepository.findByRefundId(refundId)) {
            items.add(
                    new RefundHistoryItem(
                            entry.getStatus(),
                            entry.getOccurredAt(),
                            entry.getEventId(),
                            refundEntrySource(entry.getStatus()),
                            entry.getProviderReference()));
        }

        return items.stream().sorted(Comparator.comparing(RefundHistoryItem::occurredAt)).toList();
    }

    // FUNDS_RESERVED comes from the ledger, EXPIRED from this service's own sweep; everything
    // else on the refund trail is psp-connector's.
    private static String refundEntrySource(String status) {
        if (STATUS_FUNDS_RESERVED.equals(status)) return SOURCE_LEDGER;
        if (PaymentStatus.EXPIRED.name().equals(status)) return SOURCE_PAYMENT_API;
        return SOURCE_PSP_CONNECTOR;
    }
}
