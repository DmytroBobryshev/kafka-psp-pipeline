package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.application.PaymentQueryUseCase;
import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.model.PaymentPage;
import com.example.psp.paymentapi.domain.model.PaymentStatus;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * M19's read-only web adapter for the payment hexagon: the transactions panel's backend. Kept
 * separate from {@link PaymentController} (which owns the single write verb, {@code POST}) rather
 * than folded into it - a small, deliberate CQRS-shaped split at the controller level only; both
 * still front the exact same {@link Payment} aggregate and the exact same
 * {@link PaymentWebMapper}/{@link PaymentResponse} DTOs.
 *
 * <p>Every endpoint here is a read against this service's own local projection (ADR-0005) - built
 * up over time by {@code adapters.in.kafka.PaymentStatusChangedListener} for the status column and
 * by {@link PaymentController#create} for everything else - never a call to another service, which
 * would violate ADR-0004.
 */
@RestController
@RequestMapping("/api/payments")
public class PaymentQueryController {

    /** {@code size} clamp floor - a caller asking for zero or fewer rows gets one instead. */
    private static final int MIN_SIZE = 1;

    /** {@code size} clamp ceiling - keeps one request from forcing an unbounded table scan. */
    private static final int MAX_SIZE = 100;

    private final PaymentQueryUseCase useCase;
    private final PaymentWebMapper mapper;
    private final RefundWebMapper refundMapper;
    private final PaymentHistoryWebMapper historyMapper;

    public PaymentQueryController(
            PaymentQueryUseCase useCase,
            PaymentWebMapper mapper,
            RefundWebMapper refundMapper,
            PaymentHistoryWebMapper historyMapper) {
        this.useCase = useCase;
        this.mapper = mapper;
        this.refundMapper = refundMapper;
        this.historyMapper = historyMapper;
    }

    /**
     * {@code GET /api/payments?merchantId=&status=&page=0&size=25}. {@code merchantId}/
     * {@code status} are optional filters; an unparseable {@code status} (anything that is not a
     * {@link PaymentStatus} constant) throws {@link IllegalArgumentException}, which common-web's
     * {@code GlobalExceptionHandler} turns into {@code 400 Bad Request} - the same convention
     * every other free-text-enum query parameter in this codebase relies on. {@code page}/
     * {@code size} are clamped rather than rejected: a caller passing {@code size=1000} gets 100
     * rows back, not an error, and a negative {@code page} is treated as {@code 0}.
     */
    @GetMapping
    public ResponseEntity<PaymentPageResponse> search(
            @RequestParam(name = "merchantId", required = false) String merchantId,
            @RequestParam(name = "status", required = false) String status,
            @RequestParam(name = "page", defaultValue = "0") int page,
            @RequestParam(name = "size", defaultValue = "25") int size) {

        int clampedPage = Math.max(page, 0);
        int clampedSize = Math.min(Math.max(size, MIN_SIZE), MAX_SIZE);
        PaymentStatus parsedStatus = parseStatus(status);

        PaymentPage result = useCase.search(merchantId, parsedStatus, clampedPage, clampedSize);
        List<PaymentResponse> items = result.items().stream().map(mapper::toResponse).toList();

        return ResponseEntity.ok(new PaymentPageResponse(items, result.page(), result.size(), result.total()));
    }

    /** {@code GET /api/payments/{id}} - {@code 200} with the payment, or {@code 404}. */
    @GetMapping("/{id}")
    public ResponseEntity<PaymentResponse> getById(@PathVariable("id") UUID id) {
        Payment payment = useCase.getById(id);
        return ResponseEntity.ok(mapper.toResponse(payment));
    }

    /**
     * {@code GET /api/payments/{id}/refunds} - every refund requested against this payment, via
     * the existing {@code domain.port.RefundRepository#findByPaymentId} and {@link RefundWebMapper}
     * (M11), reused unchanged here. An unknown {@code id} answers {@code 200} with an empty list -
     * see {@code PaymentQueryUseCase#listRefunds}'s javadoc for why.
     */
    @GetMapping("/{id}/refunds")
    public ResponseEntity<List<RefundResponse>> refunds(@PathVariable("id") UUID id) {
        List<RefundResponse> refunds =
                useCase.listRefunds(id).stream().map(refundMapper::toResponse).toList();
        return ResponseEntity.ok(refunds);
    }

    /**
     * {@code GET /api/payments/{id}/history} (M20) - the transactions panel's PSP state-machine
     * drill-down: {@code CREATED -> PENDING -> SUCCEEDED/FAILED}, ordered {@code occurredAt}
     * ascending. {@code 200} with {@code {"items": [...]}} (never empty - every payment has at
     * least its synthesized {@code CREATED} entry), or {@code 404} for an unknown {@code id} -
     * same {@link java.util.NoSuchElementException} convention {@link #getById} already uses,
     * propagated straight through from {@link PaymentQueryUseCase#history}.
     */
    @GetMapping("/{id}/history")
    public ResponseEntity<PaymentHistoryResponse> history(@PathVariable("id") UUID id) {
        List<PaymentHistoryItemResponse> items =
                useCase.history(id).stream().map(historyMapper::toResponse).toList();
        return ResponseEntity.ok(new PaymentHistoryResponse(items));
    }

    private PaymentStatus parseStatus(String status) {
        if (status == null || status.isBlank()) {
            return null;
        }
        try {
            return PaymentStatus.valueOf(status);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unknown payment status '" + status + "'", e);
        }
    }
}
