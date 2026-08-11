package com.example.psp.pspconnector.domain.model;

/**
 * The (simulated) provider's answer to one refund execution attempt (M11). Two-way, unlike
 * {@link ProviderOutcome}'s three-way payment vocabulary - the module brief asks the orchestrator
 * to drive exactly two deterministic paths (happy path and compensation), so this simulation does
 * not model a refund timeout; see {@code adapters.out.http.SimulatedPaymentProviderAdapter}'s
 * javadoc and services/psp-connector/README.md's M11 section for the forceable property.
 */
public enum RefundOutcome {
    COMPLETED,
    DECLINED
}
