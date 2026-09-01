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

@Service
public class PaymentQueryUseCase {

    private static final String SOURCE_PAYMENT_API = "payment-api";

    private static final String SOURCE_PSP_CONNECTOR = "psp-connector";

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

    public PaymentPage search(String merchantId, PaymentStatus status, int page, int size) {
        return paymentRepository.search(merchantId, status, page, size);
    }

    public Payment getById(UUID paymentId) {
        return paymentRepository
                .findById(paymentId)
                .orElseThrow(() -> new NoSuchElementException("No payment with id=" + paymentId));
    }

    public List<Refund> listRefunds(UUID paymentId) {
        return refundRepository.findByPaymentId(paymentId);
    }

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

    private static String refundEntrySource(String status) {
        if (STATUS_FUNDS_RESERVED.equals(status)) return SOURCE_LEDGER;
        if (PaymentStatus.EXPIRED.name().equals(status)) return SOURCE_PAYMENT_API;
        return SOURCE_PSP_CONNECTOR;
    }
}
