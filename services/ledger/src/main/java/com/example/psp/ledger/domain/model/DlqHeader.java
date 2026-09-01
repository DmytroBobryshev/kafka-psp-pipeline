package com.example.psp.ledger.domain.model;

import java.util.Arrays;
import java.util.Objects;

public record DlqHeader(String key, byte[] value) {

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DlqHeader other)) {
            return false;
        }
        return key.equals(other.key) && Arrays.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, Arrays.hashCode(value));
    }
}
