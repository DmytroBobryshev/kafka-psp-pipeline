package com.example.psp.webhooknotifier.adapters.in.web;

public record DlqReplayResponse(int replayedCount, String dlqTopic, String republishedToTopic) {}
