package com.example.psp.common.web.error;

import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * RFC 7807 ("Problem Details for HTTP APIs") error handling, shared across every service.
 *
 * <p>Every response body is a {@link ProblemDetail}: {@code type}, {@code title}, {@code status},
 * {@code detail}, {@code instance}, plus a {@code correlationId} extension property so a client
 * can hand support the same id that appears in the server logs (see {@link
 * com.example.psp.common.web.correlation.CorrelationIdFilter}).
 *
 * <p>Services extend this in M3+ with domain-specific {@code @ExceptionHandler} methods (e.g. a
 * retryable-vs-non-retryable taxonomy per ADR-0006); this base class only covers the generic
 * cases every service needs on day one.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);
    private static final URI VALIDATION_ERROR_TYPE =
            URI.create("https://psp.example.com/problems/validation-error");
    private static final URI NOT_FOUND_TYPE =
            URI.create("https://psp.example.com/problems/not-found");
    private static final URI INTERNAL_ERROR_TYPE =
            URI.create("https://psp.example.com/problems/internal-error");

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail handleIllegalArgument(IllegalArgumentException ex, HttpServletRequest request) {
        ProblemDetail problem =
                ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
        problem.setTitle("Validation error");
        problem.setType(VALIDATION_ERROR_TYPE);
        return withCommonProperties(problem, request);
    }

    @ExceptionHandler(NoSuchElementException.class)
    public ProblemDetail handleNotFound(NoSuchElementException ex, HttpServletRequest request) {
        ProblemDetail problem =
                ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
        problem.setTitle("Resource not found");
        problem.setType(NOT_FOUND_TYPE);
        return withCommonProperties(problem, request);
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleUnexpected(Exception ex, HttpServletRequest request) {
        log.error("Unhandled exception on {} {}", request.getMethod(), request.getRequestURI(), ex);
        ProblemDetail problem =
                ProblemDetail.forStatusAndDetail(
                        HttpStatus.INTERNAL_SERVER_ERROR, "An unexpected error occurred");
        problem.setTitle("Internal server error");
        problem.setType(INTERNAL_ERROR_TYPE);
        return withCommonProperties(problem, request);
    }

    private ProblemDetail withCommonProperties(ProblemDetail problem, HttpServletRequest request) {
        problem.setInstance(URI.create(request.getRequestURI()));
        String correlationId = MDC.get("correlationId");
        if (correlationId != null) {
            problem.setProperty("correlationId", correlationId);
        }
        return problem;
    }
}
