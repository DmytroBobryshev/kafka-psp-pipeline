package com.example.psp.ledger.domain.model;

public enum RefundSagaStatus {

    REQUESTED,

    RESERVED,

    COMPLETED,

    FAILED,

    RELEASED,

    NEEDS_MANUAL_REVIEW;

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == RELEASED || this == NEEDS_MANUAL_REVIEW;
    }
}
