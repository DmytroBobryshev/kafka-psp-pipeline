package com.example.psp.common.events;

import java.security.SecureRandom;
import java.util.UUID;

public final class UuidV7 {

    private static final SecureRandom RANDOM = new SecureRandom();

    private UuidV7() {
    }

    public static UUID generate() {
        return generate(System.currentTimeMillis());
    }

    static UUID generate(long unixTimeMillis) {
        byte[] value = new byte[16];
        RANDOM.nextBytes(value);

        // Bytes 0-5: 48-bit big-endian Unix timestamp in milliseconds.
        value[0] = (byte) (unixTimeMillis >>> 40);
        value[1] = (byte) (unixTimeMillis >>> 32);
        value[2] = (byte) (unixTimeMillis >>> 24);
        value[3] = (byte) (unixTimeMillis >>> 16);
        value[4] = (byte) (unixTimeMillis >>> 8);
        value[5] = (byte) unixTimeMillis;

        // Byte 6: version nibble (0111 = 7) in the high 4 bits, random in the low 4.
        value[6] = (byte) (0x70 | (value[6] & 0x0F));

        // Byte 8: variant bits (10) in the top 2 bits, random elsewhere.
        value[8] = (byte) (0x80 | (value[8] & 0x3F));

        long mostSigBits = 0;
        for (int i = 0; i < 8; i++) {
            mostSigBits = (mostSigBits << 8) | (value[i] & 0xFFL);
        }
        long leastSigBits = 0;
        for (int i = 8; i < 16; i++) {
            leastSigBits = (leastSigBits << 8) | (value[i] & 0xFFL);
        }
        return new UUID(mostSigBits, leastSigBits);
    }
}
