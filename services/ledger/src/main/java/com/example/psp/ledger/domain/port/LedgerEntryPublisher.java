package com.example.psp.ledger.domain.port;

import com.example.psp.ledger.domain.model.LedgerEntry;
import com.example.psp.ledger.domain.model.MerchantBalance;

public interface LedgerEntryPublisher {

    void publishEntryRecorded(LedgerEntry entry, MerchantBalance balanceAfter);
}
