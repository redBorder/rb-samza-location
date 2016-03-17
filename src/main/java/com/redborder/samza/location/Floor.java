package com.redborder.samza.location;

import java.util.Map;

public class Floor extends Location {

    public Floor(Long consolidatedTime, Long tGlobal, Long tLastSeen, Long tTransition, String oldLoc, String newLoc,
                 String consolidated, String entrance, String latLong) {
        super(consolidatedTime, tGlobal, tLastSeen, tTransition, oldLoc, newLoc, consolidated, entrance, latLong);
    }

    public Floor(Long consolidatedTime, Map<String, Object> rawLocation) {
        super(consolidatedTime, rawLocation);
    }
}
