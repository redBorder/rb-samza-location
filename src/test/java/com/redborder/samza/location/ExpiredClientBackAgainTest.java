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
import static com.redborder.samza.util.Dimensions.ZONE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ExpiredClientBackAgainTest extends TestCase {
    private final Logger log = LoggerFactory.getLogger(getClass().getName());
    private static Long CONSOLIDATED_TIME = 3 * MINUTE;
    private static Long EXPIRED_TIME = 30 * MINUTE;

    static List<Map<String, Object>> results;

    private static Long T1 = 1000000000L;
    private static Long T2 = T1 + CONSOLIDATED_TIME;
    private static Long T3 = T2 + EXPIRED_TIME;
    private static Long T4 = T3 + CONSOLIDATED_TIME;


    @BeforeClass
    public static void prepare() throws Exception {
        SamzaLocationTask samzaLocationTask = new SamzaLocationTask();

        Config config = mock(Config.class);
        when(config.getLong("redborder.location.consolidatedTime", CONSOLIDATED_TIME))
                .thenReturn(CONSOLIDATED_TIME);
        when(config.getLong("redborder.location.expiredTime", EXPIRED_TIME))
                .thenReturn(EXPIRED_TIME);

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
        message2.put(CAMPUS, "C1");
        message2.put(BUILDING, "B1");
        message2.put(FLOOR, "F1");
        message2.put(ZONE, "Z1");
        message2.put(LATLONG, "-33.84882,151.06793");

        IncomingMessageEnvelope envelope2 = new IncomingMessageEnvelope(
                new SystemStreamPartition("kafka", "rb_location", new Partition(0)), "OFFSET", "KEY", message2);

        samzaLocationTask.process(envelope2, collector, null);

        Map<String, Object> message3 = new HashMap<>();
        message3.put(TIMESTAMP, T4);
        message3.put(NAMESPACE, "N1");
        message3.put(CLIENT, "X1");
        message3.put(CAMPUS, "C1");
        message3.put(BUILDING, "B1");
        message3.put(FLOOR, "F1");
        message3.put(ZONE, "Z1");
        message3.put(LATLONG, "-33.84882,151.06793");

        IncomingMessageEnvelope envelope3 = new IncomingMessageEnvelope(
                new SystemStreamPartition("kafka", "rb_location", new Partition(0)), "OFFSET", "KEY", message3);

        samzaLocationTask.process(envelope3, collector, null);

        results = collector.getResult();
    }

    @Test
    public void checkSession() throws Exception {
        for (Map<String, Object> result : results.subList(0, 19)) {
            if(result.get(TYPE).equals("zone")) {
                assertEquals("X1N1-0", result.get(SESSION));
            }
        }

        for (Map<String, Object> result : results.subList(20, results.size())) {
            if(result.get(TYPE).equals("zone")) {
                assertEquals("X1N1-1", result.get(SESSION));
            }
        }
    }
}
