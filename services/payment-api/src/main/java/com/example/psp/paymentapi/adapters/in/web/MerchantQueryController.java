package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.application.MerchantQueryUseCase;
import com.example.psp.paymentapi.domain.model.MerchantPage;
import com.example.psp.paymentapi.domain.model.MerchantStatus;
import com.example.psp.paymentapi.domain.model.MerchantView;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/merchants")
public class MerchantQueryController {

    private static final int MIN_SIZE = 1;

    private static final int MAX_SIZE = 100;

    private final MerchantQueryUseCase useCase;
    private final MerchantViewWebMapper mapper;

    public MerchantQueryController(MerchantQueryUseCase useCase, MerchantViewWebMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @GetMapping
    public ResponseEntity<MerchantPageResponse> search(
            @RequestParam(name = "status", required = false) String status,
            @RequestParam(name = "page", defaultValue = "0") int page,
            @RequestParam(name = "size", defaultValue = "25") int size) {

        int clampedPage = Math.max(page, 0);
        int clampedSize = Math.min(Math.max(size, MIN_SIZE), MAX_SIZE);
        MerchantStatus parsedStatus = parseStatus(status);

        MerchantPage result = useCase.search(parsedStatus, clampedPage, clampedSize);
        List<MerchantResponse> items = result.items().stream().map(mapper::toResponse).toList();

        return ResponseEntity.ok(
                new MerchantPageResponse(items, result.page(), result.size(), result.total()));
    }

    @GetMapping("/{id}")
    public ResponseEntity<MerchantResponse> getById(@PathVariable("id") String id) {
        MerchantView merchant = useCase.getById(id);
        return ResponseEntity.ok(mapper.toResponse(merchant));
    }

    private MerchantStatus parseStatus(String status) {
        if (status == null || status.isBlank()) {
            return null;
        }
        try {
            return MerchantStatus.valueOf(status);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unknown merchant status '" + status + "'", e);
        }
    }
}
