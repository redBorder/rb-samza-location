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
public class RepeatVisitorsTest extends TestCase {
    private final Logger log = LoggerFactory.getLogger(getClass().getName());
    private static Long CONSOLIDATED_TIME = 3 * MINUTE;
    private static Long EXPIRED_TIME = 30 * MINUTE;
    static List<Map<String, Object>> results;

    static Long T1 = Utils.timestamp2Long(1459758060L);
    static Long T2 = Utils.timestamp2Long(T1 + CONSOLIDATED_TIME);
    static Long T3 = Utils.timestamp2Long(T2 + MINUTE);
    static Long T4 = Utils.timestamp2Long(T3 + CONSOLIDATED_TIME);
    static Long T5 = Utils.timestamp2Long(T4 + MINUTE);
    static Long T6 = Utils.timestamp2Long(T5 + CONSOLIDATED_TIME);

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
        when(config.getLong("redborder.location.expiredRepetitionsTime.minute", 24 * 60L * 7))
                .thenReturn(24 * 60L * 7);

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

        Map<String, Object> message2 = new HashMap<>();
        message2.put(TIMESTAMP, T3);
        message2.put(NAMESPACE, "N1");
        message2.put(CLIENT, "X1");
        message2.put(CAMPUS, "C2");
        message2.put(BUILDING, "B2");
        message2.put(FLOOR, "F2");
        message2.put(ZONE, "Z2");
        message2.put(LATLONG, "-26.84882,100.06793");

        IncomingMessageEnvelope envelope2 = new IncomingMessageEnvelope(
                new SystemStreamPartition("kafka", "rb_location", new Partition(0)), "OFFSET", "KEY", message2);

        samzaLocationTask.process(envelope2, collector, null);

        Map<String, Object> message3 = new HashMap<>();
        message3.put(TIMESTAMP, T4);
        message3.put(NAMESPACE, "N1");
        message3.put(CLIENT, "X1");
        message3.put(CAMPUS, "C2");
        message3.put(BUILDING, "B2");
        message3.put(FLOOR, "F2");
        message3.put(ZONE, "Z2");
        message3.put(LATLONG, "-26.84882,100.06793");

        IncomingMessageEnvelope envelope3 = new IncomingMessageEnvelope(
                new SystemStreamPartition("kafka", "rb_location", new Partition(0)), "OFFSET", "KEY", message3);

        samzaLocationTask.process(envelope3, collector, null);

        Map<String, Object> message4 = new HashMap<>();
        message4.put(TIMESTAMP, T5);
        message4.put(NAMESPACE, "N1");
        message4.put(CLIENT, "X1");
        message4.put(CAMPUS, "C1");
        message4.put(BUILDING, "B1");
        message4.put(FLOOR, "F1");
        message4.put(ZONE, "Z1");
        message4.put(LATLONG, "-33.84882,151.06793");

        IncomingMessageEnvelope envelope4 = new IncomingMessageEnvelope(
                new SystemStreamPartition("kafka", "rb_location", new Partition(0)), "OFFSET", "KEY", message4);

        samzaLocationTask.process(envelope4, collector, null);

        Map<String, Object> message5 = new HashMap<>();
        message5.put(TIMESTAMP, T6);
        message5.put(NAMESPACE, "N1");
        message5.put(CLIENT, "X1");
        message5.put(CAMPUS, "C1");
        message5.put(BUILDING, "B1");
        message5.put(FLOOR, "F1");
        message5.put(ZONE, "Z1");
        message5.put(LATLONG, "-33.84882,151.06793");

        IncomingMessageEnvelope envelope5 = new IncomingMessageEnvelope(
                new SystemStreamPartition("kafka", "rb_location", new Partition(0)), "OFFSET", "KEY", message5);

        samzaLocationTask.process(envelope5, collector, null);
        results = collector.getResult();
    }

    @Test
    public void checkRepetitions() throws Exception {
        for (Map<String, Object> result : results) {
            Long timestamp = (Long) result.get(TIMESTAMP);
            if(result.get(TYPE).equals(Location.LocationType.ZONE.type))
            log.info("{}", result);
           /* if ((T5 + MINUTE) <= timestamp && timestamp <= T6) {
                assertEquals(1, result.get(REPETITIONS));
            } else {
                assertEquals(0, result.get(REPETITIONS));
            }*/
        }
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
}
