package com.redborder.samza.location;

import com.redborder.samza.SamzaLocationTask;
import com.redborder.samza.util.MockMessageCollector;
import com.redborder.samza.util.MockTaskContext;
import junit.framework.TestCase;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.redborder.samza.util.Dimensions.*;
import static com.redborder.samza.util.Dimensions.SESSION;
import static com.redborder.samza.util.Dimensions.TYPE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FirstEventNullTest extends TestCase {
    private final Logger log = LoggerFactory.getLogger(getClass().getName());
    private static Long CONSOLIDATED_TIME = 3 * MINUTE;
    private static Long EXPIRED_TIME = 30 * MINUTE;
    static List<Map<String, Object>> results;

    static Long T1 = 1000000000L;
    static Long T2 = T1 + CONSOLIDATED_TIME;
    static Long T3 = T2 + CONSOLIDATED_TIME;

    @BeforeClass
    public static void prepare() throws Exception {
        SamzaLocationTask samzaLocationTask = new SamzaLocationTask();

        Config config = mock(Config.class);
        when(config.getLong("redborder.location.consolidatedTime.seconds", CONSOLIDATED_TIME))
                .thenReturn(CONSOLIDATED_TIME);
        when(config.getLong("redborder.location.expiredTime.seconds", EXPIRED_TIME))
                .thenReturn(EXPIRED_TIME);
        when(config.getLong("redborder.location.maxDwellTime.minute", 24 * 60L))
                .thenReturn(24 * 60L);

        samzaLocationTask.init(config, new MockTaskContext());

        MockMessageCollector collector = new MockMessageCollector();
        Map<String, Object> message = new HashMap<>();
        message.put(TIMESTAMP, T1);
        message.put(NAMESPACE, "N1");
        message.put(CLIENT, "X1");

        IncomingMessageEnvelope envelope = new IncomingMessageEnvelope(
                new SystemStreamPartition("kafka", "rb_location", new Partition(0)), "OFFSET", "KEY", message);

        samzaLocationTask.process(envelope, collector, null);

        Map<String, Object> message1 = new HashMap<>();
        message1.put(TIMESTAMP, T2);
        message1.put(NAMESPACE, "N1");
        message1.put(CLIENT, "X1");
        message1.put(CAMPUS, "C1");
        message1.put(BUILDING, "B1");
        message1.put(FLOOR, "F1");
        message1.put(ZONE, "Z1");
        message1.put(LATLONG, "-33.84882,151.06793");

        IncomingMessageEnvelope envelope1 = new IncomingMessageEnvelope(
                new SystemStreamPartition("kafka", "rb_location", new Partition(0)), "OFFSET", "KEY", message1);

        samzaLocationTask.process(envelope1, collector, null);

        Map<String, Object> message2 = new HashMap<>();
        message2.put(TIMESTAMP, T3);
        message2.put(NAMESPACE, "N1");
        message2.put(CLIENT, "X1");
        message2.put(CAMPUS, "C1");
        message2.put(BUILDING, "B1");
        message2.put(FLOOR, "F1");
        message2.put(ZONE, "Z1");
        message2.put(LATLONG, "-33.84882,151.06793");

        IncomingMessageEnvelope envelope2 = new IncomingMessageEnvelope(
                new SystemStreamPartition("kafka", "rb_location", new Partition(0)), "OFFSET", "KEY", message2);

        samzaLocationTask.process(envelope2, collector, null);
        results = collector.getResult();
    }

    @Test
    public void checkLocations() throws Exception {
        for (Map<String, Object> result : results) {

            if (result.get(TIMESTAMP).equals(T2)) {
                if (result.get(TYPE).equals(Location.LocationType.CAMPUS.type)) {
                    assertEquals("outside", result.get(OLD_LOC));
                    assertEquals("C1", result.get(NEW_LOC));
                }

                if (result.get(TYPE).equals(Location.LocationType.BUILDING.type)) {
                    assertEquals("outside", result.get(OLD_LOC));
                    assertEquals("B1", result.get(NEW_LOC));
                }

                if (result.get(TYPE).equals(Location.LocationType.FLOOR.type)) {
                    assertEquals("outside", result.get(OLD_LOC));
                    assertEquals("F1", result.get(NEW_LOC));
                }

                if (result.get(TYPE).equals(Location.LocationType.ZONE.type)) {
                    assertEquals("outside", result.get(OLD_LOC));
                    assertEquals("Z1", result.get(NEW_LOC));
                }
            } else {
                if (result.get(TYPE).equals(Location.LocationType.CAMPUS.type)) {
                    assertEquals("C1", result.get(OLD_LOC));
                    assertEquals("C1", result.get(NEW_LOC));
                }

                if (result.get(TYPE).equals(Location.LocationType.BUILDING.type)) {
                    assertEquals("B1", result.get(OLD_LOC));
                    assertEquals("B1", result.get(NEW_LOC));
                }

                if (result.get(TYPE).equals(Location.LocationType.FLOOR.type)) {
                    assertEquals("F1", result.get(OLD_LOC));
                    assertEquals("F1", result.get(NEW_LOC));
                }

                if (result.get(TYPE).equals(Location.LocationType.ZONE.type)) {
                    assertEquals("Z1", result.get(OLD_LOC));
                    assertEquals("Z1", result.get(NEW_LOC));
                }
            }
        }
    }
}
