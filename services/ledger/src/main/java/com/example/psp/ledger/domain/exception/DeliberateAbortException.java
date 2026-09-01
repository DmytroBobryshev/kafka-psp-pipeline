package com.example.psp.ledger.domain.exception;

import java.util.UUID;

public class DeliberateAbortException extends RuntimeException {

    public DeliberateAbortException(UUID entryId, UUID inboundEventId) {
        super(
                "ledger.fail-after-produce=true: aborting the Kafka transaction AFTER producing "
                        + "ledger.ledger-entry-recorded.v1 (entryId="
                        + entryId
                        + ", inboundEventId="
                        + inboundEventId
                        + ") and BEFORE commit - deliberate, see README 'Abort visibility proof'");
    }
}
