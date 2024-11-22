package com.redis.riot.meesho;

public class KeyPrefixUtil {

    public static byte[] intTo2ByteArray(int value) {
        byte[] byteArray = new byte[2];
        byteArray[0] = (byte) (value >> 8);   // Higher 8 bits
        byteArray[1] = (byte) (value);        // Lower 8 bits
        return byteArray;
    }

    public static int byteArrayToInt(byte[] byteArray) {
        if (byteArray == null || byteArray.length != 2) {
            throw new IllegalArgumentException("Byte array must be of size 2.");
        }
        return ((byteArray[0] & 0xFF) << 8) | (byteArray[1] & 0xFF);
    }
}
