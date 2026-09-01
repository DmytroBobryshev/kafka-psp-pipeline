package com.example.psp.paymentapi.adapters.in.web;

import jakarta.validation.constraints.NotBlank;

public record OpenDisputeRequest(
        @NotBlank(message = "reason must not be blank") String reason,
        @NotBlank(message = "documentBase64 must not be blank") String documentBase64,
        @NotBlank(message = "contentType must not be blank") String contentType) {
}
