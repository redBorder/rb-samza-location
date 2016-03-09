package com.redborder.samza.location;

import com.redborder.samza.util.Utils;

import java.util.*;

import static com.redborder.samza.util.Dimensions.*;

public class LocationData {
    Campus campus;
    Building building;
    Floor floor;
    Zone zone;

    private LocationData(Campus campus, Building building, Floor floor, Zone zone) {
        this.campus = campus;
        this.building = building;
        this.floor = floor;
        this.zone = zone;
    }

    public List<Map<String, Object>> updateWithNewLocationData(LocationData locationData) {
        List<Map<String, Object>> toSend = new LinkedList<>();

        if (campus != null) {
            toSend.addAll(campus.updateWithNewLocation(locationData.campus, Location.LocationType.CAMPUS));
        }

        if (building != null) {
            toSend.addAll(building.updateWithNewLocation(locationData.building, Location.LocationType.BUILDING));
        }

        if (floor != null) {
            toSend.addAll(floor.updateWithNewLocation(locationData.floor, Location.LocationType.FLOOR));
        }

        if (zone != null) {
            toSend.addAll(zone.updateWithNewLocation(locationData.zone, Location.LocationType.ZONE));
        }

        return toSend;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();

        if (campus != null) map.put(CAMPUS, campus.toMap());
        if (building != null) map.put(BUILDING, building.toMap());
        if (floor != null) map.put(FLOOR, floor.toMap());
        if (zone != null) map.put(ZONE, zone.toMap());

        return map;
    }

    public static LocationData locationFromCache(Long consolidatedTime, Map<String, Object> rawData) {
        LocationData.Builder builder = new LocationData.Builder();

        Map<String, Object> campusData = (Map<String, Object>) rawData.get(CAMPUS);
        if (campusData != null) {
            builder.withCampus(new Campus(consolidatedTime, campusData));
        }

        Map<String, Object> buildingData = (Map<String, Object>) rawData.get(BUILDING);
        if (buildingData != null) {
            builder.withBuilding(new Building(consolidatedTime, buildingData));
        }

        Map<String, Object> floorData = (Map<String, Object>) rawData.get(FLOOR);
        if (floorData != null) {
            builder.withFloor(new Floor(consolidatedTime, floorData));
        }

        Map<String, Object> zoneData = (Map<String, Object>) rawData.get(ZONE);
        if (zoneData != null) {
            builder.withZone(new Zone(consolidatedTime, zoneData));
        }

        return builder.build();
    }

    public static LocationData locationFromMessage(Long consolidatedTime, Map<String, Object> rawData) {
        LocationData.Builder builder = new LocationData.Builder();
        Long timestamp = Utils.timestamp2Long(rawData.get(TIMESTAMP));

        String campus = (String) rawData.get(CAMPUS);
        if (campus != null) {
            builder.withCampus(new Campus(consolidatedTime, timestamp, timestamp, "N/A", campus, "N/A", campus));
        }

        String building = (String) rawData.get(BUILDING);
        if (building != null) {
            builder.withBuilding(new Building(consolidatedTime, timestamp, timestamp, "N/A", building, "N/A", building));
        }

        String floor = (String) rawData.get(FLOOR);
        if (floor != null) {
            builder.withFloor(new Floor(consolidatedTime, timestamp, timestamp, "N/A", floor, "N/A", floor));
        }

        String zone = (String) rawData.get(ZONE);
        if (zone != null) {
            builder.withZone(new Zone(consolidatedTime, timestamp, timestamp, "N/A", zone, "N/A", zone));
        }

        return builder.build();
    }

    public static class Builder {
        Campus campus;
        Building building;
        Floor floor;
        Zone zone;

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
            return new LocationData(campus, building, floor, zone);
        }
    }
}
