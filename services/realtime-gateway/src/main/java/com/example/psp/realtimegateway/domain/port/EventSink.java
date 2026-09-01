package com.example.psp.realtimegateway.domain.port;

import com.example.psp.realtimegateway.domain.model.RealtimeEvent;

@FunctionalInterface
public interface EventSink {

    void emit(RealtimeEvent event) throws Exception;
}
