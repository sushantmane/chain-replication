package edu.sjsu.cs249.chain.util;

public class Utils {

    // convert long session id to hex session id string
    public static String getHexSid(long sessionId) {
        // return "0x" + Long.toHexString(sessionId);
        return Long.toHexString(sessionId);
    }

    // convert hex session id to long session id
    public static long getLongSid(String sessionId) {
        return Long.parseLong(sessionId, 16);
    }

    public static void main(String[] args) {
    }
}