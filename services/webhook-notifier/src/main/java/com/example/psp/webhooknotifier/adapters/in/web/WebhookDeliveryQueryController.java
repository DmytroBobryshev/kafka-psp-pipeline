package com.example.psp.webhooknotifier.adapters.in.web;

import com.example.psp.webhooknotifier.application.ListWebhookDeliveriesUseCase;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * M19's deliveries-visibility web adapter: {@code GET /api/webhooks/deliveries?paymentId=&refundId=&merchantId=&limit=25}.
 * Every parameter is optional; an unparseable {@code paymentId}/{@code refundId} (not a UUID)
 * is rejected {@code 400} by Spring's own {@code MethodArgumentTypeMismatchException} handling in
 * common-web's {@code GlobalExceptionHandler} before this method ever runs.
 *
 * <p>Kept thin by construction: every real decision - which filters apply, how attempts fold into
 * one logical delivery, what the limit clamps to - lives in
 * {@link ListWebhookDeliveriesUseCase}/{@code domain.port.DeliveryAttemptLogRepository#search},
 * not here.
 */
@RestController
@RequestMapping("/api/webhooks/deliveries")
public class WebhookDeliveryQueryController {

    private final ListWebhookDeliveriesUseCase useCase;
    private final WebhookDeliveryWebMapper mapper;

    public WebhookDeliveryQueryController(ListWebhookDeliveriesUseCase useCase, WebhookDeliveryWebMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @GetMapping
    public ResponseEntity<List<WebhookDeliveryResponse>> search(
            @RequestParam(name = "paymentId", required = false) UUID paymentId,
            @RequestParam(name = "refundId", required = false) UUID refundId,
            @RequestParam(name = "merchantId", required = false) String merchantId,
            @RequestParam(name = "limit", defaultValue = "25") int limit) {

        List<WebhookDeliveryResponse> deliveries =
                useCase.execute(paymentId, refundId, merchantId, limit).stream().map(mapper::toResponse).toList();
        return ResponseEntity.ok(deliveries);
    }
}
