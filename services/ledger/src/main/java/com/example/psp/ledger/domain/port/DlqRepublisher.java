package com.example.psp.ledger.domain.port;

import com.example.psp.ledger.domain.model.DlqRecord;

public interface DlqRepublisher {

    void republish(DlqRecord record);
}
