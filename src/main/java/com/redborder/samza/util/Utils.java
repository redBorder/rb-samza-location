package com.redborder.samza.util;

import org.apache.commons.math3.exception.NotANumberException;

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

    public static Long toLong(Object l) {
        Long result;

        if (l != null) {
            if (l instanceof Integer) {
                result = ((Integer) l).longValue();
            } else if (l instanceof Long) {
                result = (Long) l;
            } else if (l instanceof String) {
                result = Long.valueOf((String) l);
            } else {
                throw new NotANumberException();
            }
        } else {
            result = 0L;
        }

        return result;
    }
}
