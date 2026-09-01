package com.example.psp.ledger.domain.model;

public enum EntryDirection {

    CREDIT,

    DEBIT;

    public int sign() {
        return this == CREDIT ? 1 : -1;
    }
}
