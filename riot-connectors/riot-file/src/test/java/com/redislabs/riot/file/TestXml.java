package com.redislabs.riot.file;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class TestXml extends AbstractFileTest {

    @Test
    public void importHash() throws Exception {
        executeFile("/xml/import-hash.txt");
        List<String> keys = commands().keys("trade:*");
        Assertions.assertEquals(3, keys.size());
        Map<String, String> trade1 = commands().hgetall("trade:1");
        Assertions.assertEquals("XYZ0001", trade1.get("isin"));
    }


}
