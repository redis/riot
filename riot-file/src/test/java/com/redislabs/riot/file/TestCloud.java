package com.redislabs.riot.file;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCloud extends AbstractFileTest {


    @Test
    public void importGcs() throws Exception {
        executeFile("/cloud/import-gcs.txt");
        List<String> keys = commands().keys("beer:*");
        Assertions.assertEquals(4432, keys.size());
        Map<String, String> beer1 = commands().hgetall("beer:1");
        Assertions.assertEquals("Hocus Pocus", beer1.get("name"));
    }

    @Test
    public void importS3() throws Exception {
        executeFile("/cloud/import-s3.txt");
        List<String> keys = commands().keys("beer:*");
        Assertions.assertEquals(4432, keys.size());
        Map<String, String> beer1 = commands().hgetall("beer:1");
        Assertions.assertEquals("Hocus Pocus", beer1.get("name"));
    }
}
