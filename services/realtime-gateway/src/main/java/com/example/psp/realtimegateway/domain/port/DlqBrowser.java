package com.example.psp.realtimegateway.domain.port;

import com.example.psp.realtimegateway.domain.model.DlqRecordView;
import java.util.List;

public interface DlqBrowser {

    List<DlqRecordView> peekLast(String topic, int max);
}
