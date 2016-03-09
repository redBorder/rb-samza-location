package com.redborder.samza.location;

import com.redborder.samza.SamzaLocationTask;
import com.redborder.samza.util.MockMessageCollector;
import com.redborder.samza.util.MockTaskContext;
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

import junit.framework.TestCase;

import java.util.*;

import static com.redborder.samza.util.Dimensions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConsolidatedTest extends TestCase {
    private final Logger log = LoggerFactory.getLogger(getClass().getName());
    private static Long CONSOLIDATED_TIME = 3 * MINUTE;
    static List<Map<String, Object>> results;

    static Long T1 = 1457500000L;
    static Long T2 = 1457500200L;

    @BeforeClass
    public static void prepare() throws Exception {
        SamzaLocationTask samzaLocationTask = new SamzaLocationTask();

        Config config = mock(Config.class);
        when(config.getLong("redborder.location.consolidatedTime", CONSOLIDATED_TIME))
                .thenReturn(3 * MINUTE);
        when(config.getList("redborder.location.dimToEnrich", Collections.<String>emptyList()))
                .thenReturn(Collections.<String>emptyList());

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

        IncomingMessageEnvelope envelope1 = new IncomingMessageEnvelope(
                new SystemStreamPartition("kafka", "rb_location", new Partition(0)), "OFFSET", "KEY", message1);

        samzaLocationTask.process(envelope1, collector, null);
        results = collector.getResult();
    }

    @Test
    public void checkNumEvents() throws Exception {
        assertEquals(4 * Double.valueOf(Math.floor((T2 - T1 + MINUTE) / 60)).intValue(), results.size());
    }

    @Test
    public void checkTimes() throws Exception {
        Map<Long, Integer> times = new HashMap<>();

        for (Map<String, Object> result : results) {
            Long tKey = (Long) result.get(TIMESTAMP);

            if(!times.containsKey(tKey)) {
                times.put(tKey, 1);
            } else {
                times.put(tKey, times.get(tKey) + 1);
            }
        }

        for(Integer time : times.values()){
            assertEquals(Integer.valueOf(4), time);
        }

        for(Long t = T1; t <= T2; t += 60){
            assertTrue(times.containsKey(t));
        }
    }
}
