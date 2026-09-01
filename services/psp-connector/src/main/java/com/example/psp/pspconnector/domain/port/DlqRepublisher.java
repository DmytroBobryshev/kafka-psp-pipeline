package com.example.psp.pspconnector.domain.port;

import com.example.psp.pspconnector.domain.model.DlqRecord;

public interface DlqRepublisher {

    void republish(DlqRecord record);
}
