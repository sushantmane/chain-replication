package edu.sjsu.cs249.chain.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Enumeration;

public class Utils {

    /**
     * convert session id to hex session id string
     * @param sessionId
     * @return string representing hex sessionId
     */
    public static String getHexSid(long sessionId) {
        // return "0x" + Long.toHexString(sessionId);
        return Long.toHexString(sessionId);
    }

    /**
     * convert hex session id to long session id
     * @param sessionId
     * @return sessionId
     */
    public static long getLongSid(String sessionId) {
        return Long.parseLong(sessionId, 16);
    }

    /**
     * Get Ip4 address associated with given network interface
     * @param netIf network interface name
     * @return IP4 address or an empty string if fails too get the address
     */
    public static String getHostIp4Addr(String netIf) {
        String ip = "";
        try {
            NetworkInterface nif = NetworkInterface.getByName(netIf);
            Enumeration<InetAddress> addresses = nif.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress addr = addresses.nextElement();
                if (addr instanceof Inet4Address) {
                    return addr.getHostAddress();
                }
            }
        } catch (Exception ignore) {
        }
        return ip;
    }

    /**
     * Get host machine ip4 address
     * @return IP4 address or an empty string if it fails to get the address
     */
    public static String getLocalhost() {
        String ip = "";
        try {
            ip = Inet4Address.getLocalHost().getHostAddress();
        } catch (UnknownHostException ignored) {
        }
        return ip;
    }

    public static InetSocketAddress str2addr(String addr) {
        int colon = addr.lastIndexOf(':');
        return new InetSocketAddress(addr.substring(0, colon),
                Integer.parseInt(addr.substring(colon + 1 )));
    }

    public static void main(String[] args) {
        System.out.println(getLocalhost());
    }
}