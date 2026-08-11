package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.application.CheckProviderStatusUseCase;
import com.example.psp.paymentapi.domain.exception.ProviderStatusTimeoutException;
import com.example.psp.paymentapi.domain.model.ProviderStatusResult;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;
import java.util.UUID;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * M12's REST edge for the request-reply round trip: a synchronous provider-status check for a
 * payment. This is the ONE endpoint in this service that blocks the calling HTTP thread on a
 * Kafka round trip rather than returning immediately from a local database write - see
 * {@code application.CheckProviderStatusUseCase}, {@code adapters.out.kafka.ProviderStatusRequestGateway},
 * and the README's M12 section (which addresses ADR-0004 head-on: this complies with "all
 * inter-service communication is Kafka events" on the letter, while still being the synchronous
 * coupling ADR-0004 warns about - see that section for when this trade is worth it).
 *
 * <p>Deliberately thin, same shape as {@link PaymentController} - the interesting work
 * (correlation, timeout, wire format) is not here.
 */
@RestController
@RequestMapping("/api/payments")
public class ProviderStatusController {

    private final CheckProviderStatusUseCase useCase;
    private final ProviderStatusWebMapper mapper;

    public ProviderStatusController(CheckProviderStatusUseCase useCase, ProviderStatusWebMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @GetMapping("/{paymentId}/provider-status")
    public ProviderStatusResponse checkStatus(@PathVariable("paymentId") UUID paymentId) {
        ProviderStatusResult result = useCase.execute(paymentId);
        return mapper.toResponse(result);
    }

    /**
     * {@link ProviderStatusTimeoutException} maps to 504, not 500: the server did nothing wrong,
     * a downstream dependency (psp-connector, or the reply topic round trip) did not answer in
     * time - the same distinction {@code common-web.GlobalExceptionHandler}'s other handlers draw
     * between client and server faults, applied to a gateway-timeout situation that base class
     * has no reason to know about (it is specific to this one endpoint).
     */
    @ExceptionHandler(ProviderStatusTimeoutException.class)
    public ProblemDetail handleTimeout(ProviderStatusTimeoutException ex, HttpServletRequest request) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.GATEWAY_TIMEOUT, ex.getMessage());
        problem.setTitle("Provider status check timed out");
        problem.setType(URI.create("https://psp.example.com/problems/provider-status-timeout"));
        problem.setInstance(URI.create(request.getRequestURI()));
        String correlationId = MDC.get("correlationId");
        if (correlationId != null) {
            problem.setProperty("correlationId", correlationId);
        }
        return problem;
    }
}
