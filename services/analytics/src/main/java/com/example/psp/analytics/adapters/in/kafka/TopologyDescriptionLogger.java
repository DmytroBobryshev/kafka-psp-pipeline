package com.example.psp.analytics.adapters.in.kafka;

import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

@Component
public class TopologyDescriptionLogger {

    private static final Logger log = LoggerFactory.getLogger(TopologyDescriptionLogger.class);

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public TopologyDescriptionLogger(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void logTopology() {
        Topology topology = streamsBuilderFactoryBean.getTopology();
        if (topology == null) {
            log.info("Kafka Streams is not started (spring.kafka.streams.auto-startup=false?) - no topology to describe");
            return;
        }
        log.info("Kafka Streams topology:\n{}", topology.describe());
    }
}
