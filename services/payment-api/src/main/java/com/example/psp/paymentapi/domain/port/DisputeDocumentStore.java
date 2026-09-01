package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.DocumentReference;

public interface DisputeDocumentStore {

    DocumentReference store(String disputeId, byte[] documentBytes, String contentType);
}
