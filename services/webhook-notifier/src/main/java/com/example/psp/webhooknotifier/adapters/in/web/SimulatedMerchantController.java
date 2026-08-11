package com.example.psp.webhooknotifier.adapters.in.web;

import com.example.psp.webhooknotifier.config.SimulatedMerchantProperties;
import com.example.psp.webhooknotifier.config.SimulatedMerchantProperties.ForcedOutcome;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * M8's simulated merchant endpoint: a REAL HTTP endpoint {@code adapters.out.http.RestClientMerchantWebhookClient}
 * calls over the loopback interface, so a forced failure is a genuine HTTP failure (connection,
 * status code, timeout) rather than a mocked method return - exactly what the M8 brief asks for
 * ("An in-process controller the service calls over real HTTP is fine and preferable to mocking").
 *
 * <p>By default {@code webhook-notifier.merchant-client.base-url} points right back at this same
 * service ({@code http://localhost:8088}); pointing it at a real merchant's registered webhook URL
 * instead is a one-property change with zero code change on either side of the port
 * (ADR-0004's usual outbound-HTTP carve-out).
 *
 * <p>Outcome resolution order (first match wins):
 *
 * <ol>
 *   <li>{@code webhook-notifier.simulated-merchant.forced-outcome} if not {@code NONE} - forces
 *       every request regardless of merchant.
 *   <li>A {@code merchantId} containing {@code force-success}/{@code force-4xx}/{@code force-5xx}/
 *       {@code force-timeout} - forces that merchant's requests only, letting one run mix outcomes.
 *   <li>Otherwise, a weighted die roll against {@code server-error-rate}/{@code client-error-rate}/
 *       {@code timeout-rate}.
 * </ol>
 */
@RestController
@RequestMapping("/simulated-merchant")
public class SimulatedMerchantController {

    private static final Logger log = LoggerFactory.getLogger(SimulatedMerchantController.class);

    private final SimulatedMerchantProperties properties;

    public SimulatedMerchantController(SimulatedMerchantProperties properties) {
        this.properties = properties;
    }

    @PostMapping("/webhooks/{merchantId}")
    public ResponseEntity<Void> receive(
            @PathVariable("merchantId") String merchantId, @RequestBody SimulatedMerchantWebhookRequest body) {
        Outcome outcome = resolveOutcome(merchantId);
        log.info(
                "Simulated merchant received webhook merchantId={} paymentId={} status={} -> outcome={}",
                merchantId,
                body.paymentId(),
                body.status(),
                outcome);

        sleep(properties.latencyMs());

        return switch (outcome) {
            case SUCCESS -> ResponseEntity.ok().build();
            case CLIENT_ERROR -> ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
            case SERVER_ERROR -> ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            case TIMEOUT -> {
                // Sleeps PAST the caller's own read-timeout (webhook-notifier.merchant-client.read-
                // timeout-ms) on purpose, so the caller has already given up with a genuine
                // client-side timeout by the time this response would arrive. What is returned here
                // is irrelevant - nobody is listening for it - but a request handler must return
                // something.
                sleep(properties.timeoutDelayMs());
                yield ResponseEntity.ok().build();
            }
        };
    }

    private Outcome resolveOutcome(String merchantId) {
        if (properties.forcedOutcome() != ForcedOutcome.NONE) {
            return Outcome.valueOf(properties.forcedOutcome().name());
        }
        if (merchantId.contains("force-success")) {
            return Outcome.SUCCESS;
        }
        if (merchantId.contains("force-4xx")) {
            return Outcome.CLIENT_ERROR;
        }
        if (merchantId.contains("force-5xx")) {
            return Outcome.SERVER_ERROR;
        }
        if (merchantId.contains("force-timeout")) {
            return Outcome.TIMEOUT;
        }

        double roll = ThreadLocalRandom.current().nextDouble();
        if (roll < properties.serverErrorRate()) {
            return Outcome.SERVER_ERROR;
        }
        if (roll < properties.serverErrorRate() + properties.clientErrorRate()) {
            return Outcome.CLIENT_ERROR;
        }
        if (roll < properties.serverErrorRate() + properties.clientErrorRate() + properties.timeoutRate()) {
            return Outcome.TIMEOUT;
        }
        return Outcome.SUCCESS;
    }

    private static void sleep(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while simulating merchant latency", e);
        }
    }

    private enum Outcome {
        SUCCESS,
        CLIENT_ERROR,
        SERVER_ERROR,
        TIMEOUT
    }
}
