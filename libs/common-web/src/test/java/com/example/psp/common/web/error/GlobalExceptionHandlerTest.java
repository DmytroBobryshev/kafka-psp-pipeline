package com.example.psp.common.web.error;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.mock.web.MockHttpServletRequest;

class GlobalExceptionHandlerTest {

    private final GlobalExceptionHandler handler = new GlobalExceptionHandler();

    @Test
    void mapsIllegalArgumentToBadRequest() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/payments");

        ProblemDetail problem =
                handler.handleIllegalArgument(new IllegalArgumentException("amount must be positive"), request);

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST.value());
        assertThat(problem.getDetail()).isEqualTo("amount must be positive");
        assertThat(problem.getInstance()).hasToString("/api/payments");
    }

    @Test
    void mapsNoSuchElementToNotFound() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/payments/123");

        ProblemDetail problem =
                handler.handleNotFound(new NoSuchElementException("payment 123 not found"), request);

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.NOT_FOUND.value());
    }

    @Test
    void mapsUnexpectedExceptionsToInternalServerError() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/payments/123");

        ProblemDetail problem = handler.handleUnexpected(new RuntimeException("boom"), request);

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR.value());
        assertThat(problem.getDetail()).isEqualTo("An unexpected error occurred");
    }

    @Test
    void includesCorrelationIdWhenPresentInMdc() {
        org.slf4j.MDC.put("correlationId", "corr-abc");
        try {
            MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/payments/123");

            ProblemDetail problem = handler.handleNotFound(new NoSuchElementException("nope"), request);

            assertThat(problem.getProperties()).containsEntry("correlationId", "corr-abc");
        } finally {
            org.slf4j.MDC.clear();
        }
    }
}
