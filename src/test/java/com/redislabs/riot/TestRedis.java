package com.redislabs.riot;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRedis extends BaseTest {

	@Test
	public void testExportToRedis() throws Exception {
		runFile("import-beers_csv");
		List<String> keys = commands().keys("beer:*");
		Assertions.assertEquals(2410, keys.size());
		runFile("export-redis");
		List<String> keys2 = commands().keys("beer2:*");
		Assertions.assertEquals(keys.size(), keys2.size());
	}

}
