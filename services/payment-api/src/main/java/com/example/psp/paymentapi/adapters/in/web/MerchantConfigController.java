package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.application.MerchantConfigUseCase;
import com.example.psp.paymentapi.domain.model.MerchantConfig;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * The merchant-config command surface (M10). ADR-0004: commands enter the system through REST on
 * this service and nowhere else; everything downstream is events. payment-api owns this API
 * because it already owns the only externally-reachable REST surface in the system.
 *
 * <p>Two verbs, two record shapes on {@code merchants.merchant-config-changed.v1}:
 *
 * <ul>
 *   <li>{@code PUT} publishes a value - the merchant's complete configuration snapshot. It
 *       becomes the key's retained last-value under {@code cleanup.policy=compact}.</li>
 *   <li>{@code DELETE} publishes a <b>tombstone</b>: same key, {@code null} value. That null is
 *       what makes the log cleaner eventually drop every record for the key, including the
 *       tombstone itself. It is not a flag, and a flag would not work - see
 *       {@link com.example.psp.paymentapi.domain.port.MerchantConfigPublisher#publishConfigDeleted}
 *       and the README's "Merchant config" section for why.</li>
 * </ul>
 *
 * <p>{@code DELETE} returns {@code 202 Accepted}, not {@code 204 No Content}: the tombstone is
 * durably on the topic when this method returns, but "the key is gone" is not yet true anywhere -
 * the log cleaner has not run, downstream {@code GlobalKTable}s have not necessarily consumed it,
 * and the tombstone itself lingers for {@code delete.retention.ms}. 202 states that honestly;
 * 204 would claim a completed deletion that has, at that instant, deleted nothing.
 *
 * <p>Both verbs are idempotent, which is what makes them safe for a caller to retry blindly after
 * a 5xx - the failure mode the M6 outbox exists to prevent on the payment path simply does not
 * arise here.
 */
@RestController
@RequestMapping("/api/merchants/{merchantId}/config")
@Validated
public class MerchantConfigController {

    private final MerchantConfigUseCase merchantConfigUseCase;
    private final MerchantConfigWebMapper mapper;

    public MerchantConfigController(
            MerchantConfigUseCase merchantConfigUseCase, MerchantConfigWebMapper mapper) {
        this.merchantConfigUseCase = merchantConfigUseCase;
        this.mapper = mapper;
    }

    @PutMapping
    public ResponseEntity<MerchantConfigResponse> upsert(
            @PathVariable("merchantId") @NotBlank String merchantId,
            @Valid @RequestBody UpsertMerchantConfigRequest request) {

        MerchantConfig published = merchantConfigUseCase.upsert(mapper.toCommand(merchantId, request));
        // 200, not 201: PUT to a fixed URL is a replace, and on a compacted topic every write to
        // a key is a replace of that key's value whether or not one existed before. There is no
        // "created a new resource at a new location" case to report.
        return ResponseEntity.ok(mapper.toResponse(published));
    }

    @DeleteMapping
    public ResponseEntity<Void> delete(@PathVariable("merchantId") @NotBlank String merchantId) {
        merchantConfigUseCase.delete(merchantId);
        return ResponseEntity.accepted().build();
    }
}
