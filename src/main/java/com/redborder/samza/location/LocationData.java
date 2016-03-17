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
        }

        if (building != null && locationData.building != null) {
            toSend.addAll(building.updateWithNewLocation(locationData.building, Location.LocationType.BUILDING));
        }

        if (floor != null && locationData.floor != null) {
            toSend.addAll(floor.updateWithNewLocation(locationData.floor, Location.LocationType.FLOOR));
        }

        if (zone != null && locationData.zone != null) {
            toSend.addAll(zone.updateWithNewLocation(locationData.zone, Location.LocationType.ZONE));
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

    public static LocationData locationFromCache(Long consolidatedTime, Map<String, Object> rawData, String uuidPrefix) {
        LocationData.Builder builder = new LocationData.Builder();
        builder.timestamp(Utils.timestamp2Long(rawData.get(T_GLOBAL_LAST_SEEN)));

        Map<String, Object> campusData = (Map<String, Object>) rawData.get(CAMPUS);
        if (campusData != null) {
            builder.withCampus(new Campus(consolidatedTime, campusData, uuidPrefix));
        }

        Map<String, Object> buildingData = (Map<String, Object>) rawData.get(BUILDING);
        if (buildingData != null) {
            builder.withBuilding(new Building(consolidatedTime, buildingData, uuidPrefix));
        }

        Map<String, Object> floorData = (Map<String, Object>) rawData.get(FLOOR);
        if (floorData != null) {
            builder.withFloor(new Floor(consolidatedTime, floorData, uuidPrefix));
        }

        Map<String, Object> zoneData = (Map<String, Object>) rawData.get(ZONE);
        if (zoneData != null) {
            builder.withZone(new Zone(consolidatedTime, zoneData, uuidPrefix));
        }

        return builder.build();
    }

    public static LocationData locationFromMessage(Long consolidatedTime, Map<String, Object> rawData, String uuidPrefix) {
        Long timestamp = Utils.timestamp2Long(rawData.get(TIMESTAMP));
        String latLong = (String) rawData.get(LATLONG);

        LocationData.Builder builder = new LocationData.Builder();
        builder.timestamp(timestamp);

        String campus = (String) rawData.get(CAMPUS);
        if (campus != null) {
            builder.withCampus(new Campus(consolidatedTime, timestamp, timestamp, timestamp, "outside", campus, "outside", campus, latLong, uuidPrefix));
        }

        String building = (String) rawData.get(BUILDING);
        if (building != null) {
            builder.withBuilding(new Building(consolidatedTime, timestamp, timestamp, timestamp, "outside", building, "outside", building, latLong, uuidPrefix));
        }

        String floor = (String) rawData.get(FLOOR);
        if (floor != null) {
            builder.withFloor(new Floor(consolidatedTime, timestamp, timestamp, timestamp, "outside", floor, "outside", floor, latLong, uuidPrefix));
        }

        String zone = (String) rawData.get(ZONE);
        if (zone != null) {
            builder.withZone(new Zone(consolidatedTime, timestamp, timestamp, timestamp, "outside", zone, "outside", zone, latLong, uuidPrefix));
        }

        return builder.build();
    }

    public static class Builder {
        Long tGlobalLastSeen;
        Campus campus;
        Building building;
        Floor floor;
        Zone zone;

        public void timestamp(Long timestamp){
            this.tGlobalLastSeen = timestamp;
        }

        public void withCampus(Campus campus) {
            this.campus = campus;
        }

        public void withBuilding(Building building) {
            this.building = building;
        }

        public void withFloor(Floor floor) {
            this.floor = floor;
        }

        public void withZone(Zone zone) {
            this.zone = zone;
        }

        public LocationData build() {
            return new LocationData(tGlobalLastSeen, campus, building, floor, zone);
        }
    }
}
