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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.redborder.samza.util.Dimensions.*;
import static com.redborder.samza.util.Dimensions.FLOOR;
import static com.redborder.samza.util.Dimensions.ZONE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FirstEntranceTest extends TestCase{
    private final Logger log = LoggerFactory.getLogger(getClass().getName());
    private static Long CONSOLIDATED_TIME = 3 * MINUTE;
    private static Long EXPIRED_TIME = 30 * MINUTE;

    static List<Map<String, Object>> results;

    static Long T1 = 1457500000L;

    @BeforeClass
    public static void prepare() throws Exception {
        SamzaLocationTask samzaLocationTask = new SamzaLocationTask();

        Config config = mock(Config.class);
        when(config.getLong("redborder.location.consolidatedTime", CONSOLIDATED_TIME))
                .thenReturn(3 * MINUTE);
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

        results = collector.getResult();
    }

    @Test
    public void checkNoEvents() throws Exception {
        assertEquals(0, results.size());
    }

}
