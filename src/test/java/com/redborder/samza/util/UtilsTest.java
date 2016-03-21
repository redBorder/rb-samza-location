package com.redborder.samza.util;

import junit.framework.TestCase;
import org.apache.commons.math3.exception.NotANumberException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class UtilsTest extends TestCase {

    @Test
    public void null2Long() throws Exception {
        assertEquals(Long.valueOf(0L), Utils.toLong(null));
    }

    @Test
    public void int2Long() throws Exception {
        assertEquals(Long.valueOf(1L), Utils.toLong(1));
    }

    @Test
    public void string2Long() throws Exception {
        assertEquals(Long.valueOf(1L), Utils.toLong("1"));
    }

    @Test
    public void long2Long() throws Exception {
        assertEquals(Long.valueOf(1L), Long.valueOf(1L));
    }

    @Test(expected = NotANumberException.class)
    public void other2Long() throws Exception {
        Utils.toLong(new Object());
    }
}
