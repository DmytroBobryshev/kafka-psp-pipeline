package com.example.psp.ledger.adapters.in.web;

public record DlqReplayResponse(int replayedCount, String dlqTopic, String republishedToTopic) {}
