package com.example.psp.apigateway.web;

import java.net.URI;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Fallback responses for routes protected by a Resilience4j circuit breaker (see {@code
 * application-docker-compose.yml}'s {@code payment-api} route: {@code CircuitBreaker} filter,
 * {@code fallbackUri: forward:/fallback/payment-api}). Once {@code paymentApiCB} opens - see
 * this module's README "Circuit breaker" section for the exact trip conditions and how to force
 * it - every call to payment-api's routes is short-circuited here instead of waiting on (or
 * retrying against) a service that has already proven itself unavailable.
 *
 * <p>No {@code @RequestMapping(method = ...)} restriction: {@code forward:} preserves the
 * original HTTP method (payment-api's routes accept GET, POST, PUT, DELETE across {@code
 * PaymentController}/{@code RefundController}/{@code MerchantConfigController}/{@code
 * ProviderStatusController}), so this handler accepts all of them and returns the same RFC 7807
 * problem body regardless - the caller doesn't get a different fallback shape depending on which
 * payment-api endpoint it happened to hit.
 */
@RestController
public class FallbackController {

    @RequestMapping("/fallback/payment-api")
    public ResponseEntity<ProblemDetail> paymentApiFallback() {
        ProblemDetail problem =
                ProblemDetail.forStatusAndDetail(
                        HttpStatus.SERVICE_UNAVAILABLE,
                        "payment-api is not responding and the circuit breaker 'paymentApiCB' is open."
                                + " Retrying immediately will not help - wait for the configured"
                                + " wait-duration-in-open-state, or check whether payment-api is actually"
                                + " running.");
        problem.setTitle("payment-api unavailable");
        problem.setType(URI.create("about:blank"));
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(problem);
    }
}
