package com.redborder.samza.util;

public class Utils {
    public static Long timestamp2Long(Object timestamp) {
        if (timestamp != null) {
            if (timestamp instanceof Integer) {
                return ((Integer) timestamp).longValue();
            } else if (timestamp instanceof Long) {
                return (Long) timestamp;
            } else {
                return System.currentTimeMillis() / 1000L;
            }
        } else {
            return System.currentTimeMillis() / 1000L;
        }
    }
}
