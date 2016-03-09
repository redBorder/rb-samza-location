package com.redborder.samza;

import com.redborder.samza.location.LocationData;
import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.redborder.samza.util.Dimensions.*;

public class SamzaLocationTask implements StreamTask, InitableTask {

    final SystemStream systemStream = new SystemStream("kafka", "rb_loc_post");
    KeyValueStore<String, Map<String, Object>> store;
    Long consolidatedTime;
    List<String> dimToEnrich;

    @Override
    public void init(Config config, TaskContext taskContext) throws Exception {
        this.store = (KeyValueStore<String, Map<String, Object>>) taskContext.getStore("location");
        this.consolidatedTime = config.getLong("redborder.location.consolidatedTime", 3 * MINUTE);
        this.dimToEnrich = config.getList("redborder.location.dimToEnrich", Collections.<String>emptyList());
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator taskCoordinator) throws Exception {
        Map<String, Object> message = (Map<String, Object>) envelope.getMessage();
        String client = (String) message.get(CLIENT);
        String namespace = message.get(NAMESPACE) == null ? "" : (String) message.get(NAMESPACE);

        String id = client + namespace;

        if (client != null) {
            List<Map<String, Object>> events = new LinkedList<>();
            LocationData currentLocation = LocationData.locationFromMessage(consolidatedTime, message);
            Map<String, Object> cacheData = store.get(id);

            if (cacheData != null) {
                LocationData cacheLocation = LocationData.locationFromCache(consolidatedTime, cacheData);
                events.addAll(cacheLocation.updateWithNewLocationData(currentLocation));
                store.put(id, cacheLocation.toMap());
            } else {
                store.put(id, currentLocation.toMap());
            }


            for (Map<String, Object> event : events) {

                for (String dim : dimToEnrich) {
                    Object dimValue = message.get(dim);
                    if (dimValue != null) {
                        event.put(dim, dimValue);
                    }
                }

                collector.send(new OutgoingMessageEnvelope(systemStream, event.get(CLIENT), event));
            }
        }
    }
}
