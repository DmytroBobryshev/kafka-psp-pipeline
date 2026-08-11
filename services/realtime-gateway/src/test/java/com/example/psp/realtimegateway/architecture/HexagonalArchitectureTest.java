package com.example.psp.realtimegateway.architecture;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.Architectures.layeredArchitecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Enforces the hexagon (ADR-0004, ADR-0007) for realtime-gateway: {@code domain/} is pure Java,
 * and dependencies point inward only - {@code adapters -> application -> domain}. Same rule set
 * as every other service's {@code HexagonalArchitectureTest}, adapted to this service's package
 * tree and domain class names. No persistence-framework rule variant needed beyond the shared one
 * below - this service has no database at all (module brief), so it trivially passes.
 */
class HexagonalArchitectureTest {

    private static final String BASE_PACKAGE = "com.example.psp.realtimegateway";

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
                        .resideInAnyPackage("org.apache.kafka..", "..avro..")
                        .because("domain/ must not know Kafka or Avro exist (ADR-0007)");

        rule.check(classes);
    }

    @Test
    void domainMustNotDependOnPersistenceFrameworks() {
        ArchRule rule =
                noClasses()
                        .that()
                        .resideInAPackage("..domain..")
                        .should()
                        .dependOnClassesThat()
                        .resideInAnyPackage("jakarta.persistence..", "org.hibernate..")
                        .because("domain/ must not depend on JPA/Hibernate (ADR-0007) - moot here, this"
                                + " service has no database, but kept for consistency with every other"
                                + " service's test");

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
        // Sanity check: guards against someone quietly renaming the package and silently
        // disabling every rule above.
        ArchRule rule =
                classes()
                        .that()
                        .haveSimpleNameEndingWith("RealtimeEvent")
                        .or()
                        .haveSimpleNameEndingWith("SubscriptionFilter")
                        .should()
                        .resideInAPackage("..domain..");

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
