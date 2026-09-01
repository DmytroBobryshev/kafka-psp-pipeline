package com.example.psp.ledger.domain.model;

public enum RefundTransitionResult {

    APPLIED,

    ALREADY_APPLIED,

    NOT_APPLICABLE,

    ESCALATED_MANUAL_REVIEW,

    REJECTED_ILLEGAL
}
