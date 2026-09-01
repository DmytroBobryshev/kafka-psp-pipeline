package com.example.psp.pspconnector.adapters.in.web;

public record DlqReplayResponse(int replayedCount, String dlqTopic, String republishedToTopic) {}
