package com.redborder.samza.location;

import java.util.Map;

public class Campus extends Location {

    public Campus(Long consolidatedTime, Long tGlobal, Long tLastSeen, Long tTransition, String oldLoc, String newLoc,
                  String consolidated, String entrance, String latLong, String uuidPrefix) {
        super(consolidatedTime, tGlobal, tLastSeen, tTransition, oldLoc, newLoc, consolidated, entrance,
                latLong, uuidPrefix);
    }

    public Campus(Long consolidatedTime, Map<String, Object> rawLocation, String uuidPrefix) {
        super(consolidatedTime, rawLocation, uuidPrefix);
    }
}
