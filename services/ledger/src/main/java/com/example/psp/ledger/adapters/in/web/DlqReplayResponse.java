package com.example.psp.ledger.adapters.in.web;

/** Response body for {@link DlqReplayController}. */
public record DlqReplayResponse(int replayedCount, String dlqTopic, String republishedToTopic) {}
