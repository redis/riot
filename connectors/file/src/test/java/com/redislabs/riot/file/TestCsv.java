package com.redislabs.riot.file;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.sun.tools.javac.comp.Lower;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.lettuce.core.GeoArgs;

public class TestCsv extends AbstractFileTest {

	@Test
	public void importHmset() throws Exception {
		executeFile("/csv/import-hmset.txt");
		List<String> keys = sync.keys("beer:*");
		Assertions.assertEquals(COUNT, keys.size());
	}

	@Test
	public void importHmsetExclude() throws Exception {
		executeFile("/csv/import-hmset-exclude.txt");
		Map<String, String> beer1036 = sync.hgetall("beer:1036");
		Assertions.assertEquals("Lower De Boom", name(beer1036));
		Assertions.assertEquals("American Barleywine", style(beer1036));
		Assertions.assertEquals("368", beer1036.get("brewery_id"));
		Assertions.assertFalse(beer1036.containsKey("row"));
		Assertions.assertFalse(beer1036.containsKey("ibu"));
	}

	@Test
	public void importHmsetInclude() throws Exception {
		executeFile("/csv/import-hmset-include.txt");
		Map<String, String> beer1036 = sync.hgetall("beer:1036");
		Assertions.assertEquals(3, beer1036.size());
		Assertions.assertEquals("Lower De Boom", name(beer1036));
		Assertions.assertEquals("American Barleywine", style(beer1036));
		Assertions.assertEquals(0.099, abv(beer1036));
	}

	@Test
	public void importHmsetFilter() throws Exception {
		executeFile("/csv/import-hmset-filter.txt");
		List<String> keys = sync.keys("beer:*");
		Assertions.assertEquals(424, keys.size());
	}

	@Test
	public void importHmsetRegex() throws Exception {
		executeFile("/csv/import-hmset-regex.txt");
		Map<String, String> airport1 = sync.hgetall("airport:1");
		Assertions.assertEquals("Pacific", airport1.get("region"));
		Assertions.assertEquals("Port_Moresby", airport1.get("city"));
	}

	@Test
	public void importGlobHmset() throws Exception {
		executeFile("/csv/import-glob-hmset.txt");
		List<String> keys = sync.keys("beer:*");
		Assertions.assertEquals(COUNT, keys.size());
	}

	@Test
	public void importGeoadd() throws Exception {
		executeFile("/csv/import-geoadd.txt");
		Set<String> results = sync.georadius("airportgeo", -122.4194, 37.7749, 20, GeoArgs.Unit.mi);
		Assertions.assertTrue(results.contains("3469"));
		Assertions.assertTrue(results.contains("10360"));
		Assertions.assertTrue(results.contains("8982"));
	}

	@Test
	public void importProcessorHmset() throws Exception {
		executeFile("/csv/import-processor-hmset.txt");
		List<String> keys = sync.keys("event:*");
		Assertions.assertEquals(568, keys.size());
		Map<String, String> event = sync.hgetall("event:248206");
		Instant date = Instant.ofEpochMilli(Long.parseLong(event.get("EpochStart")));
		Assertions.assertTrue(date.isBefore(Instant.now()));
		long index = Long.parseLong(event.get("index"));
		Assertions.assertTrue(index > 0);
	}

	@Test
	public void importMultiCommands() throws Exception {
		executeFile("/csv/import-multi-commands.txt");
		List<String> beers = sync.keys("beer:*");
		Assertions.assertEquals(2410, beers.size());
		for (String beer : beers) {
			Map<String, String> hash = sync.hgetall(beer);
			Assertions.assertTrue(hash.containsKey("name"));
			Assertions.assertTrue(hash.containsKey("brewery_id"));
		}
		Set<String> breweries = sync.smembers("breweries");
		Assertions.assertEquals(558, breweries.size());
	}

}
