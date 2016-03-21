package com.redborder.samza;

import com.google.common.collect.Lists;
import com.redborder.samza.location.Location;
import com.redborder.samza.location.LocationData;
import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.redborder.samza.util.Dimensions.*;

public class SamzaLocationTask implements StreamTask, InitableTask, WindowableTask {
    private final Logger log = LoggerFactory.getLogger(getClass().getName());
    final SystemStream systemStream = new SystemStream("kafka", "rb_loc_post");
    KeyValueStore<String, Map<String, Object>> store;
    Long consolidatedTime;
    Long expiredTime;
    List<String> dimToEnrich = Lists.newArrayList(
            // Base dimensions
            MARKET_UUID, ORGANIZATION_UUID, ZONE_UUID, NAMESPACE_UUID,
            DEPLOYMENT_UUID, SENSOR_UUID, NAMESPACE, SERVICE_PROVIDER_UUID, BUILDING_UUID, CAMPUS_UUID, FLOOR_UUID,

            // Extra dimensions
            STATUS
    );



    @Override
    public void init(Config config, TaskContext taskContext) throws Exception {
        this.store = (KeyValueStore<String, Map<String, Object>>) taskContext.getStore("location");
        this.consolidatedTime = config.getLong("redborder.location.consolidatedTime", 3 * MINUTE);
        this.expiredTime = config.getLong("redborder.location.expiredTime", 30 * MINUTE);
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator taskCoordinator) throws Exception {
        Map<String, Object> message = (Map<String, Object>) envelope.getMessage();
        String client = (String) message.get(CLIENT);
        String namespace = message.get(NAMESPACE) == null ? "" : (String) message.get(NAMESPACE);

        String id = client + namespace;

        if (client != null) {
            List<Map<String, Object>> events = new LinkedList<>();
            LocationData currentLocation = LocationData.locationFromMessage(consolidatedTime, expiredTime, message, id);
            Map<String, Object> cacheData = store.get(id);

            log.debug("Detected client with ID[{}] and with current data [{}] and cached data [" + cacheData + "]", id, currentLocation.toMap());

            if (cacheData != null) {
                LocationData cacheLocation = LocationData.locationFromCache(consolidatedTime, expiredTime, cacheData, id);
                events.addAll(cacheLocation.updateWithNewLocationData(currentLocation));
                Map<String, Object> locationMap = cacheLocation.toMap();
                store.put(id, locationMap);
                log.debug("Updating client ID[{}] with data [{}]", id, locationMap);
            } else {
                Map<String, Object> locationMap = currentLocation.toMap();
                store.put(id, locationMap);
                log.debug("Creating client ID[{}] with data [{}]", id, locationMap);
            }


            for (Map<String, Object> event : events) {
                event.put(CLIENT, client);
                for (String dim : dimToEnrich) {
                    if (!event.containsKey(dim)) {
                        Object dimValue = message.get(dim);
                        if (dimValue != null) {
                            event.put(dim, dimValue);
                        }
                    }
                }

                collector.send(new OutgoingMessageEnvelope(systemStream, event.get(CLIENT), event));
            }
        }
    }

    @Override
    public void window(MessageCollector messageCollector, TaskCoordinator taskCoordinator) throws Exception {
        KeyValueIterator<String, Map<String, Object>> iter = store.all();
        Long currentTime = System.currentTimeMillis() / 1000L;


        while (iter.hasNext()) {
            Entry<String, Map<String, Object>> entry = iter.next();
            LocationData locationData = LocationData.locationFromCache(consolidatedTime, expiredTime, entry.getValue(), entry.getKey());

            if (currentTime - locationData.tGlobalLastSeen >= expiredTime) {
               // TODO: Sending remove clients events.
            }
        }
    }
}
