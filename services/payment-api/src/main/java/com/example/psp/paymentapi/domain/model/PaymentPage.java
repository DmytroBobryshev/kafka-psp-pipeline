package com.example.psp.paymentapi.domain.model;

import java.util.List;

/**
 * One page of a {@link Payment} search (transactions panel, M19). Pure Java, no framework
 * dependency (ADR-0007) - Spring Data's own {@code Page<T>} type never crosses the
 * {@code domain.port.PaymentRepository} boundary, exactly the same "don't leak the persistence
 * framework's shape past the port" rule {@code PaymentPersistenceMapper} already enforces for
 * {@link Payment} itself.
 *
 * @param items the page's rows, newest first (see {@code PaymentRepository#search}).
 * @param page  the zero-based page index this page actually answers (post-clamp - see
 *              {@code adapters.in.web.PaymentQueryController}).
 * @param size  the page size actually used (post-clamp).
 * @param total total number of payments matching the filter, across every page - what a UI needs
 *              to render pagination controls, not just this one page's row count.
 */
public record PaymentPage(List<Payment> items, int page, int size, long total) {
}
