package com.redborder.samza.location;

import com.redborder.samza.util.Utils;

import java.util.*;

import static com.redborder.samza.util.Dimensions.*;

public class LocationData {
    public Long tGlobalLastSeen;
    public Campus campus;
    public Building building;
    public Floor floor;
    public Zone zone;

    private LocationData(Long timestamp, Campus campus, Building building, Floor floor, Zone zone) {
        this.tGlobalLastSeen = timestamp;
        this.campus = campus;
        this.building = building;
        this.floor = floor;
        this.zone = zone;
    }

    public List<Map<String, Object>> updateWithNewLocationData(LocationData locationData) {
        List<Map<String, Object>> toSend = new LinkedList<>();
        tGlobalLastSeen = locationData.tGlobalLastSeen;

        if (campus != null && locationData.campus != null) {
            toSend.addAll(campus.updateWithNewLocation(locationData.campus, Location.LocationType.CAMPUS));
        } else if (locationData.campus != null) {
            campus = locationData.campus;
        }

        if (building != null && locationData.building != null) {
            toSend.addAll(building.updateWithNewLocation(locationData.building, Location.LocationType.BUILDING));
        } else if (locationData.building != null) {
            building = locationData.building;
        }

        if (floor != null && locationData.floor != null) {
            toSend.addAll(floor.updateWithNewLocation(locationData.floor, Location.LocationType.FLOOR));
        } else if (locationData.floor != null) {
            floor = locationData.floor;
        }

        if (zone != null && locationData.zone != null) {
            toSend.addAll(zone.updateWithNewLocation(locationData.zone, Location.LocationType.ZONE));
        } else if (locationData.zone != null) {
            zone = locationData.zone;
        }

        return toSend;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();

        map.put(T_GLOBAL_LAST_SEEN, tGlobalLastSeen);

        if (campus != null) map.put(CAMPUS, campus.toMap());
        if (building != null) map.put(BUILDING, building.toMap());
        if (floor != null) map.put(FLOOR, floor.toMap());
        if (zone != null) map.put(ZONE, zone.toMap());

        return map;
    }

    public static LocationData locationFromCache(Long consolidatedTime, Long expiredTime, Map<String, Object> rawData, String uuidPrefix) {
        LocationData.Builder builder = new LocationData.Builder();
        builder.timestamp(Utils.timestamp2Long(rawData.get(T_GLOBAL_LAST_SEEN)));

        Map<String, Object> campusData = (Map<String, Object>) rawData.get(CAMPUS);
        if (campusData != null) {
            builder.withCampus(new Campus(consolidatedTime, expiredTime, campusData, uuidPrefix));
        }

        Map<String, Object> buildingData = (Map<String, Object>) rawData.get(BUILDING);
        if (buildingData != null) {
            builder.withBuilding(new Building(consolidatedTime, expiredTime, buildingData, uuidPrefix));
        }

        Map<String, Object> floorData = (Map<String, Object>) rawData.get(FLOOR);
        if (floorData != null) {
            builder.withFloor(new Floor(consolidatedTime, expiredTime, floorData, uuidPrefix));
        }

        Map<String, Object> zoneData = (Map<String, Object>) rawData.get(ZONE);
        if (zoneData != null) {
            builder.withZone(new Zone(consolidatedTime, expiredTime, zoneData, uuidPrefix));
        }

        return builder.build();
    }

    public static LocationData locationFromMessage(Long consolidatedTime, Long expiredTime, Map<String, Object> rawData, String uuidPrefix) {
        Long timestamp = Utils.timestamp2Long(rawData.get(TIMESTAMP));
        String latLong = (String) rawData.get(LATLONG);

        LocationData.Builder builder = new LocationData.Builder();
        builder.timestamp(timestamp);

        String campus = (String) rawData.get(CAMPUS);
        if (campus != null) {
            builder.withCampus(new Campus(consolidatedTime, expiredTime, timestamp, timestamp, timestamp, "outside", campus, "outside", campus, latLong, uuidPrefix));
        }

        String building = (String) rawData.get(BUILDING);
        if (building != null) {
            builder.withBuilding(new Building(consolidatedTime, expiredTime, timestamp, timestamp, timestamp, "outside", building, "outside", building, latLong, uuidPrefix));
        }

        String floor = (String) rawData.get(FLOOR);
        if (floor != null) {
            builder.withFloor(new Floor(consolidatedTime, expiredTime, timestamp, timestamp, timestamp, "outside", floor, "outside", floor, latLong, uuidPrefix));
        }

        String zone = (String) rawData.get(ZONE);
        if (zone != null) {
            builder.withZone(new Zone(consolidatedTime, expiredTime, timestamp, timestamp, timestamp, "outside", zone, "outside", zone, latLong, uuidPrefix));
        }

        return builder.build();
    }

    private static class Builder {
        Long tGlobalLastSeen;
        Campus campus;
        Building building;
        Floor floor;
        Zone zone;

        void timestamp(Long timestamp) {
            this.tGlobalLastSeen = timestamp;
        }

        void withCampus(Campus campus) {
            this.campus = campus;
        }

        void withBuilding(Building building) {
            this.building = building;
        }

        void withFloor(Floor floor) {
            this.floor = floor;
        }

        void withZone(Zone zone) {
            this.zone = zone;
        }

        LocationData build() {
            return new LocationData(tGlobalLastSeen, campus, building, floor, zone);
        }
    }
}
