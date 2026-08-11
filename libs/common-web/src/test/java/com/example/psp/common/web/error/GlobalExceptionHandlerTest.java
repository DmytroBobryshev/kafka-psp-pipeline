package com.example.psp.common.web.error;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.servlet.http.HttpServletRequest;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.mock.http.MockHttpInputMessage;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.ErrorResponseException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

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
    void mapsBeanValidationFailureToBadRequestWithFieldErrors() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/payments");
        BeanPropertyBindingResult binding = new BeanPropertyBindingResult(new Object(), "body");
        binding.addError(
                new FieldError("body", "currency", "currency must be an ISO-4217 3-letter code"));
        // MethodArgumentNotValidException needs a MethodParameter purely to describe where the
        // failure happened. Point it at a real method rather than inventing a stub for it.
        MethodParameter parameter =
                new MethodParameter(
                        GlobalExceptionHandler.class.getDeclaredMethod(
                                "handleBeanValidation",
                                MethodArgumentNotValidException.class,
                                HttpServletRequest.class),
                        0);

        ProblemDetail problem =
                handler.handleBeanValidation(
                        new MethodArgumentNotValidException(parameter, binding), request);

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST.value());
        assertThat(problem.getDetail()).isEqualTo("Request validation failed");
        assertThat(problem.getProperties())
                .extractingByKey("errors")
                .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.MAP)
                .containsEntry("currency", "currency must be an ISO-4217 3-letter code");
    }

    @Test
    void honoursTheStatusCarriedBySpringMvcExceptions() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/does-not-exist");

        // An unmatched route reaches the catch-all, because NoResourceFoundException extends
        // ServletException rather than ErrorResponseException. It must still surface as 404.
        ProblemDetail problem =
                handler.handleUnexpected(
                        new NoResourceFoundException(HttpMethod.GET, "/api/does-not-exist"),
                        request);

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.NOT_FOUND.value());
    }

    @Test
    void mapsUnparseableBodyToBadRequestWithoutEchoingTheParserMessage() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/payments");

        ProblemDetail problem =
                handler.handleUnreadableRequest(
                        new HttpMessageNotReadableException(
                                "Unexpected character ('n' (code 110)) at [Source: line 1, column 2]",
                                new MockHttpInputMessage(new byte[0])),
                        request);

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST.value());
        assertThat(problem.getDetail()).doesNotContain("Source:").doesNotContain("code 110");
    }

    @Test
    void mapsUnsupportedMethodTo405RatherThan500() {
        MockHttpServletRequest request = new MockHttpServletRequest("DELETE", "/api/payments");

        ProblemDetail problem =
                handler.handleErrorResponse(
                        new ErrorResponseException(HttpStatus.METHOD_NOT_ALLOWED), request);

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.METHOD_NOT_ALLOWED.value());
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
