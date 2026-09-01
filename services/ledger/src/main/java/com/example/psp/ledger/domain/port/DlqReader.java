package com.example.psp.ledger.domain.port;

import com.example.psp.ledger.domain.model.DlqRecord;
import java.util.List;

public interface DlqReader {

    List<DlqRecord> pollBatch(int maxRecords);
}
