package com.example.psp.paymentapi.application;

import java.util.UUID;

public record OpenDisputeCommand(UUID paymentId, String reason, byte[] documentBytes, String contentType) {
}
