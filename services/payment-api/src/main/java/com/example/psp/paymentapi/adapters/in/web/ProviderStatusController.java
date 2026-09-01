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
