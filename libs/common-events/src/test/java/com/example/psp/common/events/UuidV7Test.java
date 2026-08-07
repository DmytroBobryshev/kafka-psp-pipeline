package com.example.psp.common.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class UuidV7Test {

    @Test
    void generatesVersion7Variant2Uuids() {
        UUID uuid = UuidV7.generate();

        assertEquals(7, uuid.version());
        assertEquals(2, uuid.variant());
    }

    @Test
    void encodesTheGivenTimestampInTheTopBits() {
        long timestampMillis = 1_700_000_000_000L; // fixed instant

        UUID uuid = UuidV7.generate(timestampMillis);

        long extracted = uuid.getMostSignificantBits() >>> 16;
        assertEquals(timestampMillis, extracted);
    }

    @Test
    void successiveTimestampsProduceNonDecreasingEncodedTime() {
        UUID earlier = UuidV7.generate(1_000L);
        UUID later = UuidV7.generate(2_000L);

        long earlierMillis = earlier.getMostSignificantBits() >>> 16;
        long laterMillis = later.getMostSignificantBits() >>> 16;

        assertTrue(laterMillis > earlierMillis);
    }
}
