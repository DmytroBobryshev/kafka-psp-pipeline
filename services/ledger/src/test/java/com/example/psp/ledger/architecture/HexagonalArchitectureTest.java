package com.example.psp.ledger.architecture;

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
 * Enforces the hexagon (ADR-0004, ADR-0007) for the ledger: {@code domain/} is pure Java, and
 * dependencies point inward only - {@code adapters -> application -> domain}. Same rule set as
 * {@code psp-connector}'s and {@code payment-api}'s, adapted to this service's package tree and
 * domain class names, plus two rules specific to M7 (see
 * {@link #domainMustNotDependOnSpringTransactions()} and
 * {@link #applicationMustNotDependOnKafkaOrTransactionApis()}).
 */
class HexagonalArchitectureTest {

    private static final String BASE_PACKAGE = "com.example.psp.ledger";

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
    void domainMustNotDependOnSpringTransactions() {
        // M7-specific. This module is *about* transactions, which makes it exactly the module where
        // a @Transactional would most plausibly drift into a domain type. The transactional
        // boundaries belong to adapters/ (the Postgres one) and to the listener container (the
        // Kafka one); domain/ describes what a ledger entry IS, not how it is committed.
        ArchRule rule =
                noClasses()
                        .that()
                        .resideInAPackage("..domain..")
                        .should()
                        .dependOnClassesThat()
                        .resideInAnyPackage(
                                "org.springframework.transaction..", "jakarta.transaction..")
                        .because(
                                "transaction management is an adapter/container concern; domain/ must "
                                        + "not carry @Transactional or any transaction API (ADR-0007)");

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
                        .because("domain/ must not depend on JPA/Hibernate (ADR-0007)");

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
    void applicationMustNotDependOnKafkaOrTransactionApis() {
        // M7-specific, and the rule that keeps the two mechanisms honest. RecordLedgerEntryUseCase
        // orchestrates a Kafka transaction and a Postgres transaction without importing either:
        // the Kafka one is opened by the listener container, the Postgres one by
        // adapters.out.persistence.LedgerWriteTransaction. If this rule ever fails, the use case
        // has started managing a transaction itself - which is precisely how the two mechanisms get
        // conflated.
        ArchRule rule =
                noClasses()
                        .that()
                        .resideInAPackage("..application..")
                        .should()
                        .dependOnClassesThat()
                        .resideInAnyPackage(
                                "org.apache.kafka..",
                                "org.springframework.kafka..",
                                "org.springframework.transaction..",
                                "jakarta.transaction..")
                        .because(
                                "application/ must not open, commit or abort a transaction itself - the "
                                        + "Kafka one belongs to the listener container and the Postgres "
                                        + "one to the persistence adapter (M7)");

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
        // Sanity check: guards against someone quietly renaming the package and silently disabling
        // every rule above.
        ArchRule rule =
                classes()
                        .that()
                        .haveSimpleNameEndingWith("LedgerEntry")
                        .or()
                        .haveSimpleNameEndingWith("MerchantBalance")
                        // M11: the refund saga's domain model.
                        .or()
                        .haveSimpleNameEndingWith("RefundReservation")
                        .or()
                        .haveSimpleNameEndingWith("RefundSagaState")
                        .or()
                        .haveSimpleNameEndingWith("RefundRequest")
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
