package com.example.psp.pspconnector.domain.model;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public record DlqRecord(String key, byte[] value, List<DlqHeader> headers) {

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DlqRecord other)) {
            return false;
        }
        return key.equals(other.key) && Arrays.equals(value, other.value) && headers.equals(other.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, Arrays.hashCode(value), headers);
    }
}
