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
        return ResponseEntity.ok(mapper.toResponse(published));
    }

    @DeleteMapping
    public ResponseEntity<Void> delete(@PathVariable("merchantId") @NotBlank String merchantId) {
        merchantConfigUseCase.delete(merchantId);
        return ResponseEntity.accepted().build();
    }
}
