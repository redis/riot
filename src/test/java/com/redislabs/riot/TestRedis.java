package com.redislabs.riot;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRedis extends BaseTest {

	@Test
	public void testExportToRedis() throws Exception {
		runFile("file-import-csv-hash");
		List<String> keys = commands().keys("beer:*");
		Assertions.assertEquals(BEER_COUNT, keys.size());
		runFile("redis-export");
		List<String> keys2 = commands().keys("beer2:*");
		Assertions.assertEquals(keys.size(), keys2.size());
	}

}
