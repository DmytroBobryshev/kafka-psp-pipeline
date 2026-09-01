package com.example.psp.pspconnector.domain.port;

import com.example.psp.pspconnector.domain.model.DlqRecord;
import java.util.List;

public interface DlqReader {

    List<DlqRecord> pollBatch(int maxRecords);
}
