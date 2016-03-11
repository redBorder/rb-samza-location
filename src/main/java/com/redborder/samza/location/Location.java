package com.redborder.samza.location;

import com.redborder.samza.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.redborder.samza.util.Dimensions.*;

public class Location {
    private final Logger log = LoggerFactory.getLogger(getClass().getName());

    Long consolidatedTime;
    Long tGlobal;
    Long tLastSeen;
    Long tTransition;
    Integer dWellTime;
    String oldLoc;
    String newLoc;
    String consolidated;
    String entrance;

    enum LocationType {
        CAMPUS("campus"), BUILDING("building"), FLOOR("floor"), ZONE("zone");

        public String type;

        LocationType(String type) {
            this.type = type;
        }
    }

    public Location(Long consolidatedTime, Long tGlobal, Long tLastSeen, Long tTransition, String oldLoc, String newLoc,
                    String consolidated, String entrance) {
        this.consolidatedTime = consolidatedTime;
        this.tGlobal = tGlobal;
        this.tLastSeen = tLastSeen;
        this.tTransition = tTransition;
        this.oldLoc = oldLoc;
        this.newLoc = newLoc;
        this.consolidated = consolidated;
        this.entrance = entrance;
        this.dWellTime = 1;
    }

    public Location(Long consolidatedTime, Map<String, Object> rawLocation) {
        this.consolidatedTime = consolidatedTime;
        this.tGlobal = Utils.timestamp2Long(rawLocation.get(T_GLOBAL));
        this.tLastSeen = Utils.timestamp2Long(rawLocation.get(T_LAST_SEEN));
        this.tTransition = Utils.timestamp2Long(rawLocation.get(T_TRANSITION));
        this.dWellTime = (Integer) rawLocation.get(DWELL_TIME);
        this.oldLoc = (String) rawLocation.get(OLD_LOC);
        this.newLoc = (String) rawLocation.get(NEW_LOC);
        this.consolidated = (String) rawLocation.get(CONSOLIDATED);
        this.entrance = (String) rawLocation.get(ENTRANCE);
    }

    public List<Map<String, Object>> updateWithNewLocation(Location location, LocationType locationType) {
        List<Map<String, Object>> toSend = new LinkedList<>();

        if (newLoc.equals(location.newLoc)) {
            if (consolidated.equals(location.newLoc)) {
                for (long t = tLastSeen; t <= location.tLastSeen; t += MINUTE) {
                    Map<String, Object> event = new HashMap<>();
                    event.put(TIMESTAMP, t);
                    event.put(OLD_LOC, location.newLoc);
                    event.put(NEW_LOC, location.newLoc);
                    event.put(DWELL_TIME, dWellTime);
                    event.put(TYPE, locationType.type);
                    toSend.add(event);
                    dWellTime++;
                }

                log.info("Consolidated state, sending [{}] events", toSend.size());
                tLastSeen = location.tLastSeen;
            } else {
                if (location.tLastSeen - tLastSeen >= consolidatedTime) {

                    // Check if it's the first move!
                    if (consolidated.equals("N/A")) {
                        Map<String, Object> event = new HashMap<>();
                        event.put(TIMESTAMP, tGlobal);
                        event.put(OLD_LOC, consolidated);
                        event.put(NEW_LOC, entrance);
                        event.put(DWELL_TIME, 1);
                        event.put(TYPE, locationType.type);
                        toSend.add(event);

                        consolidated = entrance;
                        tTransition += MINUTE;
                    } else {
                        // Last Consolidated location
                        for (long t = tGlobal; t <= (tTransition - MINUTE); t += MINUTE) {
                            Map<String, Object> event = new HashMap<>();
                            event.put(TIMESTAMP, t);
                            event.put(OLD_LOC, consolidated);
                            event.put(NEW_LOC, consolidated);
                            event.put(DWELL_TIME, dWellTime);
                            event.put(TYPE, locationType.type);
                            toSend.add(event);
                            dWellTime++;
                        }
                    }

                    dWellTime = 1;
                    // Transition
                    for (long t = tTransition; t <= tLastSeen; t += MINUTE) {
                        Map<String, Object> event = new HashMap<>();
                        event.put(TIMESTAMP, t);
                        event.put(OLD_LOC, consolidated);
                        event.put(NEW_LOC, location.newLoc);
                        event.put(DWELL_TIME, dWellTime);
                        event.put(TYPE, locationType.type);
                        toSend.add(event);
                        dWellTime++;
                    }

                    dWellTime = 1;
                    // New Consolidated location
                    for (long t = (tLastSeen + MINUTE); t <= location.tLastSeen; t += MINUTE) {
                        Map<String, Object> event = new HashMap<>();
                        event.put(TIMESTAMP, t);
                        event.put(OLD_LOC, location.newLoc);
                        event.put(NEW_LOC, location.newLoc);
                        event.put(DWELL_TIME, dWellTime);
                        event.put(TYPE, locationType.type);
                        toSend.add(event);
                        dWellTime++;
                    }

                    log.info("Consolidating state, sending [{}] events", toSend.size());

                    tGlobal = location.tLastSeen;
                    tLastSeen = location.tLastSeen;
                    tTransition = location.tTransition;
                    oldLoc = location.newLoc;
                    newLoc = location.newLoc;
                    consolidated = location.newLoc;
                } else {
                    log.info("Trying to consolidate state, but {}",
                            String.format("location.tLastSeen[%s] - tLastSeen[%s] < consolidatedTime[%s]",
                                    location.tLastSeen, tLastSeen, consolidatedTime));
                }
            }
        } else {
            log.info("Moving from [{}] to [{}]", newLoc, location.newLoc);
            tLastSeen = location.tLastSeen;
            oldLoc = newLoc;
            newLoc = location.newLoc;

            // Leaving consolidated location.
            if (oldLoc.equals(consolidated)) {
                tTransition = location.tLastSeen;
            }
        }

        return toSend;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put(T_GLOBAL, tGlobal);
        map.put(T_LAST_SEEN, tLastSeen);
        map.put(T_TRANSITION, tTransition);
        map.put(DWELL_TIME, dWellTime);
        map.put(OLD_LOC, oldLoc);
        map.put(NEW_LOC, newLoc);
        map.put(CONSOLIDATED, consolidated);
        map.put(ENTRANCE, entrance);
        return map;
    }
}
