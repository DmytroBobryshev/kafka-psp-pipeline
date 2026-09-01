package com.example.psp.pspconnector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class PspConnectorApplication {

    public static void main(String[] args) {
        SpringApplication.run(PspConnectorApplication.class, args);
    }
}
