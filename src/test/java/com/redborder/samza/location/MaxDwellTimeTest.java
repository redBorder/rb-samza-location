package com.redborder.samza.location;

import com.redborder.samza.SamzaLocationTask;
import com.redborder.samza.util.MockMessageCollector;
import com.redborder.samza.util.MockTaskContext;
import com.redborder.samza.util.Utils;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MaxDwellTimeTest extends TestCase {
    private final Logger log = LoggerFactory.getLogger(getClass().getName());
    private static Long CONSOLIDATED_TIME = 3 * MINUTE;
    private static Long EXPIRED_TIME = 30 * MINUTE;
    private static Long MAX_DWELL_TIME = 24 * 60L;
    static List<Map<String, Object>> results;

    static Long T1 = Utils.timestamp2Long(1000000000L);
    static Long T2 = Utils.timestamp2Long(T1 + CONSOLIDATED_TIME);

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
        message.put(CAMPUS, "C1");
        message.put(BUILDING, "B1");
        message.put(FLOOR, "F1");
        message.put(ZONE, "Z1");
        message.put(LATLONG, "-33.84882,151.06793");

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

        // 48h * 60 min * 60 sec
        for (long t = MINUTE; t <= 48 * 60 * 60; t += MINUTE) {
            Map<String, Object> messageAux = new HashMap<>();
            messageAux.put(TIMESTAMP, T2 + t);
            messageAux.put(NAMESPACE, "N1");
            messageAux.put(CLIENT, "X1");
            messageAux.put(CAMPUS, "C1");
            messageAux.put(BUILDING, "B1");
            messageAux.put(FLOOR, "F1");
            messageAux.put(ZONE, "Z1");
            messageAux.put(LATLONG, "-33.84882,151.06793");

            samzaLocationTask.process(new IncomingMessageEnvelope(
                    new SystemStreamPartition("kafka", "rb_location", new Partition(0)), "OFFSET", "KEY", messageAux), collector, null);
        }

        results = collector.getResult();
    }

    @Test
    public void checkTimes() throws Exception {
        Map<Long, Integer> times = new HashMap<>();

        for (Map<String, Object> result : results) {
            Long tKey = (Long) result.get(TIMESTAMP);

            if (!times.containsKey(tKey)) {
                times.put(tKey, 1);
            } else {
                times.put(tKey, times.get(tKey) + 1);
            }
        }

        for (Integer time : times.values()) {
            assertEquals(Integer.valueOf(4), time);
        }
    }

    @Test
    public void maxDwell() throws Exception {
        for (Map<String, Object> result : results) {
            assertTrue(MAX_DWELL_TIME >= (Integer) result.get(DWELL_TIME));
        }

    }
}
