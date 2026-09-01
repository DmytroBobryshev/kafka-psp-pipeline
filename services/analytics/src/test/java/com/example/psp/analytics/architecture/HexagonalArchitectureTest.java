package com.example.psp.analytics.architecture;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.Architectures.layeredArchitecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class HexagonalArchitectureTest {

    private static final String BASE_PACKAGE = "com.example.psp.analytics";

    private static JavaClasses classes;

    @BeforeAll
    static void importClasses() {
        classes =
                new ClassFileImporter()
                        .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                        .importPackages(BASE_PACKAGE);
    }

    @Test
    void domainMustNotDependOnSpring() {
        ArchRule rule =
                noClasses()
                        .that()
                        .resideInAPackage("..domain..")
                        .should()
                        .dependOnClassesThat()
                        .resideInAnyPackage("org.springframework..")
                        .because("domain/ must compile with zero framework dependencies (PLAN.md)");

        rule.check(classes);
    }

    @Test
    void domainMustNotDependOnKafka() {
        ArchRule rule =
                noClasses()
                        .that()
                        .resideInAPackage("..domain..")
                        .should()
                        .dependOnClassesThat()
                        .resideInAnyPackage("org.apache.kafka..", "..avro..", "io.confluent..")
                        .because("domain/ must not know Kafka, Avro or Schema Registry exist (ADR-0007)");

        rule.check(classes);
    }

    @Test
    void domainAndApplicationMustNotDependOnKafkaStreams() {
        ArchRule rule =
                noClasses()
                        .that()
                        .resideInAnyPackage("..domain..", "..application..")
                        .should()
                        .dependOnClassesThat()
                        .resideInAnyPackage("org.apache.kafka..", "org.springframework.kafka..")
                        .because(
                                "the topology is an inbound adapter; domain/ and application/ must be "
                                        + "expressible - and testable - without Kafka Streams (M10)");

        rule.check(classes);
    }

    @Test
    void domainMustNotDependOnMongo() {
        ArchRule rule =
                noClasses()
                        .that()
                        .resideInAPackage("..domain..")
                        .should()
                        .dependOnClassesThat()
                        .resideInAnyPackage("org.springframework.data..", "com.mongodb..")
                        .because(
                                "the MongoDB projection is one adapter behind a port; the domain must "
                                        + "not know a document store exists (ADR-0005, ADR-0007)");

        rule.check(classes);
    }

    @Test
    void onlyTheTopologyMayDependOnGeneratedAvro() {
        ArchRule rule =
                noClasses()
                        .that()
                        .resideOutsideOfPackages(BASE_PACKAGE + ".adapters..", BASE_PACKAGE + ".config..")
                        .should()
                        .dependOnClassesThat()
                        .resideInAPackage("com.example.psp.common.events.avro..")
                        .because("generated Avro types are a wire contract and stop at the adapter (ADR-0007)");

        rule.check(classes);
    }

    @Test
    void domainMustNotDependOnAdapters() {
        ArchRule rule =
                noClasses()
                        .that()
                        .resideInAPackage("..domain..")
                        .should()
                        .dependOnClassesThat()
                        .resideInAPackage("..adapters..")
                        .because("domain/ defines ports; adapters implement them, never the reverse");

        rule.check(classes);
    }

    @Test
    void applicationMustNotDependOnAdapters() {
        ArchRule rule =
                noClasses()
                        .that()
                        .resideInAPackage("..application..")
                        .should()
                        .dependOnClassesThat()
                        .resideInAPackage("..adapters..")
                        .because(
                                "application/ orchestrates ports only; it must not know which adapter "
                                        + "implements them (ADR-0007)");

        rule.check(classes);
    }

    @Test
    void inboundAdaptersMustNotDependOnOutboundAdapters() {
        ArchRule rule =
                noClasses()
                        .that()
                        .resideInAPackage("..adapters.in..")
                        .should()
                        .dependOnClassesThat()
                        .resideInAPackage("..adapters.out..")
                        .because("adapters never reference each other (ADR-0007)");

        rule.check(classes);
    }

    @Test
    void outboundAdaptersMustNotDependOnInboundAdapters() {
        ArchRule rule =
                noClasses()
                        .that()
                        .resideInAPackage("..adapters.out..")
                        .should()
                        .dependOnClassesThat()
                        .resideInAPackage("..adapters.in..")
                        .because("adapters never reference each other (ADR-0007)");

        rule.check(classes);
    }

    @Test
    void domainClassesShouldOnlyResideInDomainPackage() {
        ArchRule rule =
                classes()
                        .that()
                        .haveSimpleNameEndingWith("MerchantWindowMetrics")
                        .or()
                        .haveSimpleNameEndingWith("PaymentOutcome")
                        .or()
                        .haveSimpleNameEndingWith("MerchantConfigSnapshot")
                        .or()
                        .haveSimpleNameEndingWith("AuthorizationLatency")
                        .or()
                        .haveSimpleNameEndingWith("PaymentStatusAuditEntry")
                        .should()
                        .resideInAPackage("..domain..")
                        .because("the domain model lives in domain/, whatever else is named after it");

        rule.check(classes);
    }

    @Test
    void layeredArchitectureIsRespected() {
        layeredArchitecture()
                .consideringAllDependencies()
                .layer("Domain")
                .definedBy(BASE_PACKAGE + ".domain..")
                .layer("Application")
                .definedBy(BASE_PACKAGE + ".application..")
                .layer("Adapters")
                .definedBy(BASE_PACKAGE + ".adapters..")
                .layer("Config")
                .definedBy(BASE_PACKAGE + ".config..")
                .whereLayer("Domain")
                .mayOnlyBeAccessedByLayers("Application", "Adapters", "Config")
                .whereLayer("Application")
                .mayOnlyBeAccessedByLayers("Adapters", "Config")
                .whereLayer("Adapters")
                .mayOnlyBeAccessedByLayers("Config")
                .check(classes);
    }
}
