package com.redborder.samza.util;

public class Utils {
    public static Long timestamp2Long(Object timestamp) {
        Long result;

        if (timestamp != null) {
            if (timestamp instanceof Integer) {
                result = ((Integer) timestamp).longValue();
            } else if (timestamp instanceof Long) {
                result = (Long) timestamp;
            } else {
                result = System.currentTimeMillis() / 1000L;
            }
        } else {
            result = System.currentTimeMillis() / 1000L;
        }

        return result;
    }
}
