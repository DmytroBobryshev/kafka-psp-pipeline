package com.example.psp.analytics.adapters.in.kafka;

import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

/**
 * Logs {@code Topology#describe()} once, at startup (M10).
 *
 * <p>Kafka Streams does not print the topology on its own, and the printed form is the single
 * most useful artefact this application produces for understanding itself: it names every
 * processor node, shows which stores are attached to which node, and - the part that matters for
 * this module - lists the <b>sub-topologies</b> and any {@code Sink}/{@code Source} pair pointing
 * at a {@code -repartition} topic. If a future change introduces a shuffle (a {@code selectKey},
 * a {@code groupBy}, a {@code join} between two differently-keyed streams), it shows up here as a
 * second sub-topology before it shows up as a new topic in the cluster.
 *
 * <p>Reads the built {@link Topology} from {@link StreamsBuilderFactoryBean} rather than calling
 * {@code StreamsBuilder#build()} again - the factory bean owns that call, and building twice is
 * not something the DSL promises to tolerate. Listening for {@code ApplicationReadyEvent} rather
 * than {@code @PostConstruct} for the same reason: the topology does not exist until the factory
 * bean has started.
 */
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
