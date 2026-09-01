package com.example.psp.ledger.domain.port;

import com.example.psp.ledger.domain.model.LedgerEntry;
import com.example.psp.ledger.domain.model.ReleaseOutcome;
import com.example.psp.ledger.domain.model.ReserveOutcome;
import com.example.psp.ledger.domain.model.RefundReservation;
import com.example.psp.ledger.domain.model.RefundSagaState;
import com.example.psp.ledger.domain.model.SettleOutcome;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface RefundRepository {

    boolean hasProcessedInboundEvent(UUID inboundEventId);

    ReserveOutcome tryReserveOrFail(
            RefundReservation reservation, UUID inboundEventId, String insufficientBalanceReason);

    Optional<RefundSagaState> findSagaState(UUID refundId);

    SettleOutcome trySettle(UUID refundId, LedgerEntry debitEntry, UUID inboundEventId);

    ReleaseOutcome tryRelease(UUID refundId, String reason, UUID inboundEventId);

    List<RefundSagaState> findReservedOlderThan(Instant cutoff);

    ReleaseOutcome tryReleaseForTimeout(UUID refundId);
}
