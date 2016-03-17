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
    String latLong;

    enum LocationType {
        CAMPUS("campus"), BUILDING("building"), FLOOR("floor"), ZONE("zone");

        public String type;

        LocationType(String type) {
            this.type = type;
        }
    }

    public Location(Long consolidatedTime, Long tGlobal, Long tLastSeen, Long tTransition, String oldLoc, String newLoc,
                    String consolidated, String entrance, String latLong) {
        this.consolidatedTime = consolidatedTime;
        this.tGlobal = tGlobal;
        this.tLastSeen = tLastSeen;
        this.tTransition = tTransition;
        this.oldLoc = oldLoc;
        this.newLoc = newLoc;
        this.consolidated = consolidated;
        this.entrance = entrance;
        this.dWellTime = 1;
        this.latLong = latLong;
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
        this.latLong = (String) rawLocation.get(LATLONG);
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
                    event.put(locationWithUuid(locationType), location.newLoc);
                    event.put(TYPE, locationType.type);

                    if (location.latLong != null) {
                        event.put(LATLONG, location.latLong);
                    }

                    toSend.add(event);
                    dWellTime++;
                }

                log.debug("Consolidated state, sending [{}] events", toSend.size());
                tLastSeen = location.tLastSeen;
            } else {
                if (location.tLastSeen - tLastSeen >= consolidatedTime) {

                    // Check if it's the first move!
                    if (consolidated.equals("outside")) {
                        Map<String, Object> event = new HashMap<>();
                        event.put(TIMESTAMP, tGlobal);
                        event.put(OLD_LOC, consolidated);
                        event.put(NEW_LOC, entrance);
                        event.put(locationWithUuid(locationType), entrance);
                        event.put(TRANSITION, 1);
                        event.put(DWELL_TIME, 1);
                        event.put(TYPE, locationType.type);

                        if (location.latLong != null) {
                            event.put(LATLONG, latLong);
                        }

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
                            event.put(locationWithUuid(locationType), consolidated);
                            event.put(TRANSITION, 0);
                            event.put(TYPE, locationType.type);

                            if (location.latLong != null) {
                                event.put(LATLONG, latLong);
                            }

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
                        event.put(locationWithUuid(locationType), location.newLoc);
                        event.put(DWELL_TIME, dWellTime);
                        event.put(TRANSITION, 1);
                        event.put(TYPE, locationType.type);

                        if (location.latLong != null) {
                            event.put(LATLONG, location.latLong);
                        }

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
                        event.put(locationWithUuid(locationType), location.newLoc);
                        event.put(DWELL_TIME, dWellTime);
                        event.put(TRANSITION, 0);
                        event.put(TYPE, locationType.type);

                        if (location.latLong != null) {
                            event.put(LATLONG, location.latLong);
                        }

                        toSend.add(event);
                        dWellTime++;
                    }

                    log.debug("Consolidating state, sending [{}] events", toSend.size());

                    tGlobal = location.tLastSeen;
                    tLastSeen = location.tLastSeen;
                    tTransition = location.tTransition;
                    oldLoc = location.newLoc;
                    newLoc = location.newLoc;
                    consolidated = location.newLoc;
                    latLong = location.latLong;
                } else {
                    log.debug("Trying to consolidate state, but {}",
                            String.format("location.tLastSeen[%s] - tLastSeen[%s] < consolidatedTime[%s]",
                                    location.tLastSeen, tLastSeen, consolidatedTime));
                }
            }
        } else {
            log.debug("Moving from [{}] to [{}]", newLoc, location.newLoc);
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

        if (latLong != null) {
            map.put(LATLONG, latLong);
        }

        return map;
    }

    private String locationWithUuid(LocationType type) {
        return type.type + "_uuid";
    }
}
