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

@RestController
@RequestMapping("/api/payments")
public class PaymentQueryController {

    private static final int MIN_SIZE = 1;

    private static final int MAX_SIZE = 100;

    private final PaymentQueryUseCase useCase;
    private final PaymentWebMapper mapper;
    private final RefundWebMapper refundMapper;
    private final PaymentHistoryWebMapper historyMapper;
    private final RefundHistoryWebMapper refundHistoryMapper;

    public PaymentQueryController(
            PaymentQueryUseCase useCase,
            PaymentWebMapper mapper,
            RefundWebMapper refundMapper,
            PaymentHistoryWebMapper historyMapper,
            RefundHistoryWebMapper refundHistoryMapper) {
        this.useCase = useCase;
        this.mapper = mapper;
        this.refundMapper = refundMapper;
        this.historyMapper = historyMapper;
        this.refundHistoryMapper = refundHistoryMapper;
    }

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

    @GetMapping("/{id}")
    public ResponseEntity<PaymentResponse> getById(@PathVariable("id") UUID id) {
        Payment payment = useCase.getById(id);
        return ResponseEntity.ok(mapper.toResponse(payment));
    }

    @GetMapping("/{id}/refunds")
    public ResponseEntity<List<RefundResponse>> refunds(@PathVariable("id") UUID id) {
        List<RefundResponse> refunds =
                useCase.listRefunds(id).stream().map(refundMapper::toResponse).toList();
        return ResponseEntity.ok(refunds);
    }

    @GetMapping("/{id}/history")
    public ResponseEntity<PaymentHistoryResponse> history(@PathVariable("id") UUID id) {
        List<PaymentHistoryItemResponse> items =
                useCase.history(id).stream().map(historyMapper::toResponse).toList();
        return ResponseEntity.ok(new PaymentHistoryResponse(items));
    }

    @GetMapping("/{paymentId}/refunds/{refundId}/history")
    public ResponseEntity<RefundHistoryResponse> refundHistory(
            @PathVariable("paymentId") UUID paymentId, @PathVariable("refundId") UUID refundId) {
        List<RefundHistoryItemResponse> items =
                useCase.refundHistory(paymentId, refundId).stream().map(refundHistoryMapper::toResponse).toList();
        return ResponseEntity.ok(new RefundHistoryResponse(items));
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
