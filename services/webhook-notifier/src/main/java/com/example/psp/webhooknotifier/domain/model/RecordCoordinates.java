package com.example.psp.webhooknotifier.domain.model;

import java.time.Instant;

public record RecordCoordinates(String topic, int partition, long offset, Instant timestamp) {}
