package com.redislabs.riot.file;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.lettuce.core.GeoArgs;

public class TestCsv extends AbstractFileTest {

	@Test
	public void importHmset() throws Exception {
		executeFile("/csv/import-hmset.txt");
		List<String> keys = commands().keys("beer:*");
		Assertions.assertEquals(COUNT, keys.size());
	}

	@Test
	public void importGlobHmset() throws Exception {
		executeFile("/csv/import-glob-hmset.txt");
		List<String> keys = commands().keys("beer:*");
		Assertions.assertEquals(COUNT, keys.size());
	}

	@Test
	public void importGeoadd() throws Exception {
		executeFile("/csv/import-geoadd.txt");
		Set<String> results = commands().georadius("airportgeo", -122.4194, 37.7749, 20, GeoArgs.Unit.mi);
		Assertions.assertTrue(results.contains("3469"));
		Assertions.assertTrue(results.contains("10360"));
		Assertions.assertTrue(results.contains("8982"));
	}

	@Test
	public void importProcessorHmset() throws Exception {
		executeFile("/csv/import-processor-hmset.txt");
		List<String> keys = commands().keys("event:*");
		Assertions.assertEquals(568, keys.size());
		Map<String, String> event = commands().hgetall("event:248206");
		Instant date = Instant.ofEpochMilli(Long.parseLong(event.get("EpochStart")));
		Assertions.assertTrue(date.isBefore(Instant.now()));
		long index = Long.parseLong(event.get("index"));
		Assertions.assertTrue(index > 0);
	}

	@Test
	public void importMultiCommands() throws Exception {
		executeFile("/csv/import-multi-commands.txt");
		List<String> beers = commands().keys("beer:*");
		Assertions.assertEquals(2410, beers.size());
		for (String beer : beers) {
			Map<String, String> hash = commands().hgetall(beer);
			Assertions.assertTrue(hash.containsKey("name"));
			Assertions.assertTrue(hash.containsKey("brewery_id"));
		}
		Set<String> breweries = commands().smembers("breweries");
		Assertions.assertEquals(558, breweries.size());
	}

}
