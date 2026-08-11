package com.example.psp.common.web.error;

import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.TypeMismatchException;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.validation.ObjectError;
import org.springframework.web.ErrorResponse;
import org.springframework.web.ErrorResponseException;
import org.springframework.web.bind.MethodArgumentNotValidException;
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

    /**
     * Bean-validation failure on an {@code @Valid @RequestBody}. Without this, the catch-all below
     * reports a malformed request as a 500, which tells the caller the server broke when in fact
     * their payload did - and buries the field that was actually wrong.
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleBeanValidation(
            MethodArgumentNotValidException ex, HttpServletRequest request) {
        Map<String, String> fieldErrors = new LinkedHashMap<>();
        for (FieldError error : ex.getBindingResult().getFieldErrors()) {
            fieldErrors.putIfAbsent(error.getField(), error.getDefaultMessage());
        }
        for (ObjectError error : ex.getBindingResult().getGlobalErrors()) {
            fieldErrors.putIfAbsent(error.getObjectName(), error.getDefaultMessage());
        }
        ProblemDetail problem =
                ProblemDetail.forStatusAndDetail(
                        HttpStatus.BAD_REQUEST, "Request validation failed");
        problem.setTitle("Validation error");
        problem.setType(VALIDATION_ERROR_TYPE);
        problem.setProperty("errors", fieldErrors);
        log.debug(
                "Validation failure on {} {}: {}",
                request.getMethod(),
                request.getRequestURI(),
                fieldErrors);
        return withCommonProperties(problem, request);
    }

    /**
     * A body that is not parseable at all - truncated JSON, wrong content type, a string where a
     * number belongs. Unlike most Spring MVC exceptions this one does NOT implement {@link
     * ErrorResponse}, so it reaches the catch-all and would otherwise be reported as a 500. The
     * caller sent something the parser could not read; that is a 400.
     *
     * <p>The parser's own message is deliberately not echoed back - it carries type names and
     * offsets that are noise to a caller and detail to an attacker.
     */
    @ExceptionHandler({HttpMessageNotReadableException.class, TypeMismatchException.class})
    public ProblemDetail handleUnreadableRequest(Exception ex, HttpServletRequest request) {
        log.debug(
                "Unreadable request on {} {}: {}",
                request.getMethod(),
                request.getRequestURI(),
                ex.getMessage());
        ProblemDetail problem =
                ProblemDetail.forStatusAndDetail(
                        HttpStatus.BAD_REQUEST, "Request body or parameter could not be parsed");
        problem.setTitle("Malformed request");
        problem.setType(VALIDATION_ERROR_TYPE);
        return withCommonProperties(problem, request);
    }

    /**
     * Spring MVC's own exceptions - unmatched route, unsupported method, missing parameter -
     * already carry the correct status. The catch-all below cannot see that, so without this
     * handler a 404 or a 405 is reported as a 500.
     *
     * <p>These are client errors, so they are logged at DEBUG. Logging them at ERROR (as the
     * catch-all does) means a scan for a missing favicon looks identical to a broken service.
     */
    @ExceptionHandler(ErrorResponseException.class)
    public ProblemDetail handleErrorResponse(
            ErrorResponseException ex, HttpServletRequest request) {
        return fromStatus(ex.getStatusCode(), ex, request);
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleUnexpected(Exception ex, HttpServletRequest request) {
        // Some Spring exceptions implement ErrorResponse without extending ErrorResponseException
        // (NoResourceFoundException extends ServletException, for one), so they land here. Honour
        // the status they carry rather than flattening everything to 500.
        if (ex instanceof ErrorResponse errorResponse) {
            return fromStatus(errorResponse.getStatusCode(), ex, request);
        }
        log.error("Unhandled exception on {} {}", request.getMethod(), request.getRequestURI(), ex);
        ProblemDetail problem =
                ProblemDetail.forStatusAndDetail(
                        HttpStatus.INTERNAL_SERVER_ERROR, "An unexpected error occurred");
        problem.setTitle("Internal server error");
        problem.setType(INTERNAL_ERROR_TYPE);
        return withCommonProperties(problem, request);
    }

    private ProblemDetail fromStatus(
            HttpStatusCode status, Exception ex, HttpServletRequest request) {
        if (status.is5xxServerError()) {
            log.error("Server error on {} {}", request.getMethod(), request.getRequestURI(), ex);
        } else {
            log.debug(
                    "Client error {} on {} {}: {}",
                    status.value(),
                    request.getMethod(),
                    request.getRequestURI(),
                    ex.getMessage());
        }
        HttpStatus resolved = HttpStatus.resolve(status.value());
        ProblemDetail problem = ProblemDetail.forStatus(status);
        problem.setTitle(resolved != null ? resolved.getReasonPhrase() : "Request failed");
        if (status.value() == HttpStatus.NOT_FOUND.value()) {
            problem.setType(NOT_FOUND_TYPE);
        } else if (status.is4xxClientError()) {
            problem.setType(VALIDATION_ERROR_TYPE);
        } else {
            problem.setType(INTERNAL_ERROR_TYPE);
        }
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
