package com.example.psp.common.web.config;

import com.example.psp.common.web.correlation.CorrelationIdFilter;
import com.example.psp.common.web.error.GlobalExceptionHandler;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Import;

@AutoConfiguration
@Import({CorrelationIdFilter.class, GlobalExceptionHandler.class})
public class CommonWebAutoConfiguration {
}
