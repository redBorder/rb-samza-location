package com.redborder.samza.location;

import java.util.*;

import static com.redborder.samza.util.Dimensions.*;

public class Location {
    Long consolidatedTime;
    Long tGlobal;
    Long tLastSeen;
    String oldLoc;
    String newLoc;
    String consolidated;
    String entrance;

    enum LocationType {
        CAMPUS("campus"), BUILDING("building"), FLOOR("floor"), ZONE("zone");

        private String type;

        private LocationType(String type) {
            this.type = type;
        }
    }

    public Location(Long consolidatedTime, Long tGlobal, Long tLastSeen, String oldLoc, String newLoc,
                    String consolidated, String entrance) {
        this.consolidatedTime = consolidatedTime;
        this.tGlobal = tGlobal;
        this.tLastSeen = tLastSeen;
        this.oldLoc = oldLoc;
        this.newLoc = newLoc;
        this.consolidated = consolidated;
        this.entrance = entrance;
    }

    public Location(Long consolidatedTime, Map<String, Object> rawLocation) {
        this.consolidatedTime = consolidatedTime;
        this.tGlobal = (Long) rawLocation.get(T_GLOBAL);
        this.tLastSeen = (Long) rawLocation.get(T_LAST_SEEN);
        this.oldLoc = (String) rawLocation.get(OLD_LOC);
        this.newLoc = (String) rawLocation.get(NEW_LOC);
        this.consolidated = (String) rawLocation.get(CONSOLIDATED);
        this.entrance = (String) rawLocation.get(ENTRANCE);
    }

    public List<Map<String, Object>> updateWithNewLocation(Location location, LocationType locationType) {
        List<Map<String, Object>> toSend = new LinkedList<>();

        if (location.newLoc.equals(newLoc)) {
            if (location.consolidated.equals(newLoc)) {
                for (long t = tLastSeen; t <= location.tLastSeen; t += MINUTE) {
                    Map<String, Object> event = new HashMap<>();
                    event.put(TIMESTAMP, t);
                    event.put(OLD_LOC, location.newLoc);
                    event.put(NEW_LOC, location.newLoc);
                    event.put(TYPE, locationType.type);
                    toSend.add(event);
                }

                tLastSeen = location.tLastSeen;
            } else {
                if (location.tLastSeen - tGlobal >= consolidatedTime) {
                    Map<String, Object> consolidatedMoving = new HashMap<>();
                    consolidatedMoving.put(TIMESTAMP, tGlobal);

                    if(!oldLoc.equals("N/A")) {
                        consolidatedMoving.put(OLD_LOC, oldLoc);
                    } else {
                        consolidatedMoving.put(OLD_LOC, entrance);
                    }

                    consolidatedMoving.put(NEW_LOC, location.newLoc);
                    consolidatedMoving.put(TYPE, locationType.type);
                    toSend.add(consolidatedMoving);

                    for (long t = (tGlobal + MINUTE); t <= location.tLastSeen; t += MINUTE) {
                        Map<String, Object> event = new HashMap<>();
                        event.put(TIMESTAMP, t);
                        event.put(OLD_LOC, location.newLoc);
                        event.put(NEW_LOC, location.newLoc);
                        event.put(TYPE, locationType.type);
                        toSend.add(event);
                    }

                    tGlobal = location.tLastSeen;
                    tLastSeen = location.tLastSeen;
                    oldLoc = location.newLoc;
                    newLoc = location.newLoc;
                    consolidated = location.newLoc;
                }
            }
        } else {
            tLastSeen = location.tLastSeen;
            oldLoc = newLoc;
            newLoc = location.newLoc;
        }

        return toSend;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put(T_GLOBAL, tGlobal);
        map.put(T_LAST_SEEN, tLastSeen);
        map.put(OLD_LOC, oldLoc);
        map.put(NEW_LOC, newLoc);
        map.put(CONSOLIDATED, consolidated);
        map.put(ENTRANCE, entrance);
        return map;
    }
}
