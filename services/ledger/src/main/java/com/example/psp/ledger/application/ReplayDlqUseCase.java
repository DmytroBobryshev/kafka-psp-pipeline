package com.example.psp.ledger.application;

import com.example.psp.ledger.domain.model.DlqRecord;
import com.example.psp.ledger.domain.port.DlqReader;
import com.example.psp.ledger.domain.port.DlqRepublisher;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ReplayDlqUseCase {

    private static final Logger log = LoggerFactory.getLogger(ReplayDlqUseCase.class);

    private final DlqReader dlqReader;
    private final DlqRepublisher republisher;

    public ReplayDlqUseCase(DlqReader dlqReader, DlqRepublisher republisher) {
        this.dlqReader = dlqReader;
        this.republisher = republisher;
    }

    public int replay(int maxRecords) {
        List<DlqRecord> records = dlqReader.pollBatch(maxRecords);
        for (DlqRecord record : records) {
            log.info("Replaying DLQ record key(merchantId)={}", record.key());
            republisher.republish(record);
        }
        log.info("DLQ replay complete: {} record(s) republished", records.size());
        return records.size();
    }
}
