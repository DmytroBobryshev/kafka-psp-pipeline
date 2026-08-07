package com.example.psp.common.web.config;

import com.example.psp.common.web.correlation.CorrelationIdFilter;
import com.example.psp.common.web.error.GlobalExceptionHandler;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Import;

/**
 * Registers {@code common-web}'s beans in every consuming service, regardless of that service's
 * own base package for component scanning.
 *
 * <p>{@code libs/common-events} and {@code libs/common-web} live under {@code
 * com.example.psp.common.*}, while each service scans from its own root (e.g. {@code
 * com.example.psp.paymentapi}) - plain {@code @Component} scanning would never find
 * {@link CorrelationIdFilter} or {@link GlobalExceptionHandler}. Spring Boot's auto-configuration
 * mechanism (declared in {@code META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports})
 * sidesteps that: any service that simply depends on this jar gets both beans for free.
 */
@AutoConfiguration
@Import({CorrelationIdFilter.class, GlobalExceptionHandler.class})
public class CommonWebAutoConfiguration {
}
