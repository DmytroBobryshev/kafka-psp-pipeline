package com.example.psp.analytics.adapters.in.web;

public record StreamsStateResponse(
        String applicationId, String stateDir, String clientState, boolean storeReady) {
}
