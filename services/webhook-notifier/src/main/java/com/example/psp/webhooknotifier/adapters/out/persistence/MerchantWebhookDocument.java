package com.example.psp.webhooknotifier.adapters.out.persistence;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "merchant_webhooks")
public class MerchantWebhookDocument {

    @Id private String merchantId;
    private String webhookUrl;
    private Instant updatedAt;
}
