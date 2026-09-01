package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.Refund;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface RefundRepository {

    Refund save(Refund refund);

    BigDecimal sumRequestedAmount(UUID paymentId);

    List<Refund> findByPaymentId(UUID paymentId);

    Optional<Refund> findByIdAndPaymentId(UUID id, UUID paymentId);

    List<Refund> findExpirationCandidates(Instant now);
}
