package com.redis.riot;

public class SpelUtils {

    public static String parseHex(String hexVal) {
        if (hexVal == null) {
            return null;
        }
        if (hexVal.startsWith("0x")) {
            hexVal = hexVal.substring(2);
        }
        byte[] bytes = new byte[hexVal.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            byte b = (byte) Integer.parseInt(hexVal.substring(2 * i, 2 * i + 2), 16);
            bytes[i] = b;
        }

        return new String(bytes);
    }

    public static String parseUuid(String uuid) {
        if (uuid == null) {
            return null;
        }

        java.util.UUID uuidObj = java.util.UUID.fromString(uuid);
        byte[] bytes = new byte[16];
        long msb = uuidObj.getMostSignificantBits();
        long lsb = uuidObj.getLeastSignificantBits();
        for (int i = 0; i < 8; i++) {
            bytes[i] = (byte) (msb >>> (8 * (7 - i)));
            bytes[8 + i] = (byte) (lsb >>> (8 * (7 - i)));
        }

        return new String(bytes);
    }
}
