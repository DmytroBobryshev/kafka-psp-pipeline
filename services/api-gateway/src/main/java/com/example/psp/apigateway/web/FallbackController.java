package com.example.psp.apigateway.web;

import java.net.URI;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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
