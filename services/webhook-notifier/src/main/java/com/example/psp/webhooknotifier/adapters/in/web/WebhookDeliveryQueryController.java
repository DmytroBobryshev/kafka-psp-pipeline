package com.example.psp.webhooknotifier.adapters.in.web;

import com.example.psp.webhooknotifier.application.ListWebhookDeliveriesUseCase;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
