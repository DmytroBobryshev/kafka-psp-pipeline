package com.example.psp.ledger.domain.port;

import com.example.psp.ledger.domain.model.LedgerEntry;
import com.example.psp.ledger.domain.model.MerchantBalance;
import java.util.Optional;
import java.util.UUID;

public interface LedgerRepository {

    boolean existsByInboundEventId(UUID inboundEventId);

    Optional<MerchantBalance> tryApply(LedgerEntry entry);
}
