package com.redborder.samza.location;

import java.util.Map;

public class Campus extends Location {
    public Campus(Long consolidatedTime, Long tGlobal, Long tLastSeen, Long tTransition, String oldLoc, String newLoc, String consolidated, String entrance) {
        super(consolidatedTime, tGlobal, tLastSeen, tTransition, oldLoc, newLoc, consolidated, entrance);
    }

    public Campus(Long consolidatedTime, Map<String, Object> rawLocation) {
        super(consolidatedTime, rawLocation);
    }
}
