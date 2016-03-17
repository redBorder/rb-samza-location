package com.redborder.samza.location;

import java.util.Map;

public class Floor extends Location {
    public Floor(Long consolidatedTime, Long expiredTime, Long tGlobal, Long tLastSeen, Long tTransition,
                  String oldLoc, String newLoc, String consolidated, String entrance, String latLong, String uuidPrefix) {
        super(consolidatedTime, expiredTime, tGlobal, tLastSeen, tTransition, oldLoc, newLoc, consolidated, entrance,
                latLong, uuidPrefix);
    }

    public Floor(Long consolidatedTime, Long expiredTime, Map<String, Object> rawLocation, String uuidPrefix) {
        super(consolidatedTime, expiredTime, rawLocation, uuidPrefix);
    }
}
