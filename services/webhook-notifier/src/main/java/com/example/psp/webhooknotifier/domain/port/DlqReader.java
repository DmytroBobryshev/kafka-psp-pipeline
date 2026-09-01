package com.example.psp.webhooknotifier.domain.port;

import com.example.psp.webhooknotifier.domain.model.DlqRecord;
import java.util.List;

public interface DlqReader {

    List<DlqRecord> pollBatch(int maxRecords);
}
