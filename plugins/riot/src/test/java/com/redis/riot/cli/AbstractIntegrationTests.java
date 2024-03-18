package com.redis.riot.cli;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.util.FileCopyUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.Document;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.SearchResults;
import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.search.SuggetOptions;
import com.redis.lettucemod.search.TextField.PhoneticMatcher;
import com.redis.lettucemod.timeseries.MRangeOptions;
import com.redis.lettucemod.timeseries.RangeResult;
import com.redis.lettucemod.timeseries.TimeRange;
import com.redis.riot.redis.GeneratorImport;
import com.redis.spring.batch.gen.GeneratorItemReader;

import io.lettuce.core.GeoArgs;
import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.ParseResult;

@SuppressWarnings("unchecked")
abstract class AbstractIntegrationTests extends AbstractRiotTestBase {

	public static final int BEER_CSV_COUNT = 2410;
	public static final int BEER_JSON_COUNT = 216;

	protected static String name(Map<String, String> beer) {
		return beer.get("name");
	}

	protected static String style(Map<String, String> beer) {
		return beer.get("style");
	}

	protected static double abv(Map<String, String> beer) {
		return Double.parseDouble(beer.get("abv"));
	}

	protected void testImport(TestInfo info, String filename, String pattern, int count) throws Exception {
		execute(info, filename);
		Assertions.assertEquals(count, keyCount(pattern));
	}

	@Test
	void fileImportFW(TestInfo info) throws Exception {
		testImport(info, "file-import-fw", "account:*", 5);
		Map<String, String> account101 = commands.hgetall("account:101");
		// Account LastName FirstName Balance CreditLimit AccountCreated Rating
		// 101 Reeves Keanu 9315.45 10000.00 1/17/1998 A
		Assertions.assertEquals("Reeves", account101.get("LastName"));
		Assertions.assertEquals("Keanu", account101.get("FirstName"));
		Assertions.assertEquals("A", account101.get("Rating"));
	}

	@Test
	void fileImportCSV(TestInfo info) throws Exception {
		testImport(info, "file-import-csv", "beer:*", BEER_CSV_COUNT);
	}

	@Test
	void fileImportCSVSkipLines(TestInfo info) throws Exception {
		testImport(info, "file-import-csv-skiplines", "beer:*", BEER_CSV_COUNT - 10);
	}

	@Test
	void fileImportCSVMax(TestInfo info) throws Exception {
		testImport(info, "file-import-csv-max", "beer:*", 12);
	}

	@Test
	void fileImportPSV(TestInfo info) throws Exception {
		testImport(info, "file-import-psv", "sample:*", 3);
	}

	@Test
	void fileImportTSV(TestInfo info) throws Exception {
		testImport(info, "file-import-tsv", "sample:*", 4);
	}

	@Test
	void fileImportType(TestInfo info) throws Exception {
		testImport(info, "file-import-type", "sample:*", 3);
	}

	@Test
	void fileImportExclude(TestInfo info) throws Exception {
		execute(info, "file-import-exclude");
		Map<String, String> beer1036 = commands.hgetall("beer:1036");
		Assertions.assertEquals("Lower De Boom", name(beer1036));
		Assertions.assertEquals("American Barleywine", style(beer1036));
		Assertions.assertEquals("368", beer1036.get("brewery_id"));
		Assertions.assertFalse(beer1036.containsKey("row"));
		Assertions.assertFalse(beer1036.containsKey("ibu"));
	}

	@Test
	void fileImportInclude(TestInfo info) throws Exception {
		execute(info, "file-import-include");
		Map<String, String> beer1036 = commands.hgetall("beer:1036");
		Assertions.assertEquals(3, beer1036.size());
		Assertions.assertEquals("Lower De Boom", name(beer1036));
		Assertions.assertEquals("American Barleywine", style(beer1036));
		Assertions.assertEquals(0.099, abv(beer1036));
	}

	@Test
	void fileImportFilter(TestInfo info) throws Exception {
		testImport(info, "file-import-filter", "beer:*", 424);
	}

	@Test
	void fileImportRegex(TestInfo info) throws Exception {
		execute(info, "file-import-regex");
		Map<String, String> airport1 = commands.hgetall("airport:1");
		Assertions.assertEquals("Pacific", airport1.get("region"));
		Assertions.assertEquals("Port_Moresby", airport1.get("city"));
	}

	@Test
	void fileImportGlob(TestInfo info) throws Exception {
		execute(info, "file-import-glob", this::executeImportGlob);
		Assertions.assertEquals(BEER_CSV_COUNT, keyCount("beer:*"));
	}

	private int executeImportGlob(ParseResult parseResult) {

		FileImportCommand command = command(parseResult);
		try {
			Path dir = Files.createTempDirectory("import-glob");
			FileCopyUtils.copy(getClass().getClassLoader().getResourceAsStream("files/beers1.csv"),
					Files.newOutputStream(dir.resolve("beers1.csv")));
			FileCopyUtils.copy(getClass().getClassLoader().getResourceAsStream("files/beers2.csv"),
					Files.newOutputStream(dir.resolve("beers2.csv")));
			File file = new File(command.args.files.get(0));
			command.args.files = Arrays.asList(dir.resolve(file.getName()).toString());
		} catch (IOException e) {
			throw new RuntimeException("Could not configure import-glob", e);
		}
		return ExitCode.OK;
	}

	@Test
	void fileImportGeoadd(TestInfo info) throws Exception {
		execute(info, "file-import-geoadd");
		Set<String> results = commands.georadius("airportgeo", -21, 64, 200, GeoArgs.Unit.mi);
		Assertions.assertTrue(results.contains("18"));
		Assertions.assertTrue(results.contains("19"));
		Assertions.assertTrue(results.contains("11"));
	}

	@Test
	void fileImportGeoProcessor(TestInfo info) throws Exception {
		execute(info, "file-import-geo-processor");
		Map<String, String> airport3469 = commands.hgetall("airport:18");
		Assertions.assertEquals("-21.9405994415,64.1299972534", airport3469.get("location"));
	}

	@Test
	void fileImportProcess(TestInfo info) throws Exception {
		testImport(info, "file-import-process", "event:*", 568);
		Map<String, String> event = commands.hgetall("event:248206");
		Instant date = Instant.ofEpochMilli(Long.parseLong(event.get("EpochStart")));
		Assertions.assertTrue(date.isBefore(Instant.now()));
	}

	@Test
	void fileImportProcessVar(TestInfo info) throws Exception {
		testImport(info, "file-import-process-var", "event:*", 568);
		Map<String, String> event = commands.hgetall("event:248206");
		int randomInt = Integer.parseInt(event.get("randomInt"));
		Assertions.assertTrue(randomInt >= 0 && randomInt < 100);
	}

	@Test
	void fileImportProcessElvis(TestInfo info) throws Exception {
		testImport(info, "file-import-process-elvis", "beer:*", BEER_CSV_COUNT);
		Map<String, String> beer1436 = commands.hgetall("beer:1436");
		Assertions.assertEquals("10", beer1436.get("ibu"));
	}

	@Test
	void fileImportMultiCommands(TestInfo info) throws Exception {
		execute(info, "file-import-multi-commands");
		List<String> beers = commands.keys("beer:*");
		Assertions.assertEquals(BEER_CSV_COUNT, beers.size());
		for (String beer : beers) {
			Map<String, String> hash = commands.hgetall(beer);
			Assertions.assertTrue(hash.containsKey("name"));
			Assertions.assertTrue(hash.containsKey("brewery_id"));
		}
		Set<String> breweries = commands.smembers("breweries");
		Assertions.assertEquals(558, breweries.size());
	}

	@Test
	void fileImportBad(TestInfo info) throws Exception {
		Assertions.assertEquals(0, execute(info, "file-import-bad"));
	}

	@Test
	void fileImportGCS(TestInfo info) throws Exception {
		testImport(info, "file-import-gcs", "beer:*", 4432);
		Map<String, String> beer1 = commands.hgetall("beer:1");
		Assertions.assertEquals("Hocus Pocus", name(beer1));
	}

	@Test
	void fileImportS3(TestInfo info) throws Exception {
		testImport(info, "file-import-s3", "beer:*", 4432);
		Map<String, String> beer1 = commands.hgetall("beer:1");
		Assertions.assertEquals("Hocus Pocus", name(beer1));
	}

	@Test
	@Disabled("Needs update")
	void fileImportJSONElastic(TestInfo info) throws Exception {
		execute(info, "file-import-json-elastic");
		Assertions.assertEquals(2, keyCount("estest:*"));
		Map<String, String> doc1 = commands.hgetall("estest:doc1");
		Assertions.assertEquals("ruan", doc1.get("_source.name"));
		Assertions.assertEquals("3", doc1.get("_source.articles[1]"));
	}

	@Test
	void fileImportJSON(TestInfo info) throws Exception {
		testImport(info, "file-import-json", "beer:*", BEER_JSON_COUNT);
		Map<String, String> beer1 = commands.hgetall("beer:1");
		Assertions.assertEquals("Hocus Pocus", beer1.get("name"));
	}

	@Test
	void fileImportXML(TestInfo info) throws Exception {
		testImport(info, "file-import-xml", "trade:*", 3);
		Map<String, String> trade1 = commands.hgetall("trade:1");
		Assertions.assertEquals("XYZ0001", trade1.get("isin"));
	}

	@Test
	void fileImportJSONGzip(TestInfo info) throws Exception {
		testImport(info, "file-import-json-gz", "beer:*", BEER_JSON_COUNT);
	}

	@Test
	void fileImportSugadd(TestInfo info) throws Exception {
		assertExecutionSuccessful(execute(info, "file-import-sugadd"));
		List<Suggestion<String>> suggestions = commands.ftSugget("names", "Bea",
				SuggetOptions.builder().withPayloads(true).build());
		Assertions.assertEquals(5, suggestions.size());
		Assertions.assertEquals("American Blonde Ale", suggestions.get(0).getPayload());
	}

	@Test
	void fileImportElasticJSON(TestInfo info) throws Exception {
		assertExecutionSuccessful(execute(info, "file-import-json-elastic-jsonset"));
		Assertions.assertEquals(2, keyCount("elastic:*"));
		ObjectMapper mapper = new ObjectMapper();
		String doc1 = commands.jsonGet("elastic:doc1");
		String expected = "{\"_index\":\"test-index\",\"_type\":\"docs\",\"_id\":\"doc1\",\"_score\":1,\"_source\":{\"name\":\"ruan\",\"age\":30,\"articles\":[\"1\",\"3\"]}}";
		Assertions.assertEquals(mapper.readTree(expected), mapper.readTree(doc1));
	}

	@Test
	void fakerImportHset(TestInfo info) throws Exception {
		testImport(info, "faker-import-hset", "person:*", 1000);
		Map<String, String> person = commands.hgetall("person:123");
		Assertions.assertTrue(person.containsKey("firstName"));
		Assertions.assertTrue(person.containsKey("lastName"));
		Assertions.assertTrue(person.containsKey("address"));
	}

	@Test
	void fakerImportThreads(TestInfo info) throws Exception {
		testImport(info, "faker-import-threads", "person:*", 8000);
		String key;
		do {
			key = commands.randomkey();
		} while (key == null);
		Map<String, String> person = commands.hgetall(key);
		Assertions.assertTrue(person.containsKey("firstName"));
		Assertions.assertTrue(person.containsKey("lastName"));
		Assertions.assertTrue(person.containsKey("address"));
	}

	@Test
	void fakerImportSadd(TestInfo info) throws Exception {
		execute(info, "faker-import-sadd");
		Set<String> names = commands.smembers("got:characters");
		Assertions.assertTrue(names.size() > 10);
		for (String name : names) {
			Assertions.assertFalse(name.isEmpty());
		}
	}

	@Test
	void fakerImportZadd(TestInfo info) throws Exception {
		execute(info, "faker-import-zadd");
		Assertions.assertTrue(keyCount("leases:*") > 100);
		String key;
		do {
			key = commands.randomkey();
		} while (key == null);
		Assertions.assertTrue(commands.zcard(key) > 0);
	}

	@Test
	void fakerImportXadd(TestInfo info) throws Exception {
		execute(info, "faker-import-xadd");
		List<StreamMessage<String, String>> messages = commands.xrange("teststream:1", Range.unbounded());
		Assertions.assertTrue(messages.size() > 0);
	}

	@Test
	@Disabled("Needs update")
	void fakerInfer(TestInfo info) throws Exception {
		String INDEX = "beerIdx";
		String FIELD_ID = "id";
		String FIELD_ABV = "abv";
		String FIELD_NAME = "name";
		String FIELD_STYLE = "style";
		String FIELD_OUNCES = "ounces";
		commands.ftCreate(INDEX, CreateOptions.<String, String>builder().prefix("beer:").build(),
				Field.tag(FIELD_ID).sortable().build(), Field.text(FIELD_NAME).sortable().build(),
				Field.text(FIELD_STYLE).matcher(PhoneticMatcher.ENGLISH).sortable().build(),
				Field.numeric(FIELD_ABV).sortable().build(), Field.numeric(FIELD_OUNCES).sortable().build());
		execute(info, "faker-import-infer");
		SearchResults<String, String> results = commands.ftSearch(INDEX, "*");
		Assertions.assertEquals(1000, results.getCount());
		Document<String, String> doc1 = results.get(0);
		Assertions.assertNotNull(doc1.get(FIELD_ABV));
	}

	@Test
	@Disabled("Flaky test")
	void fakerImportTsAdd(TestInfo info) throws Exception {
		execute(info, "faker-import-tsadd");
		Assertions.assertEquals(10, commands.tsRange("ts:gen", TimeRange.unbounded()).size());
	}

	@Test
	void fakerImportTsAddWithOptions(TestInfo info) throws Exception {
		execute(info, "faker-import-tsadd-options");
		List<RangeResult<String, String>> results = commands.tsMrange(TimeRange.unbounded(),
				MRangeOptions.<String, String>filters("character1=Einstein").build());
		Assertions.assertFalse(results.isEmpty());
	}

	@Test
	void generateTypes(TestInfo info) throws Exception {
		execute(info, "generate");
		Assertions.assertEquals(Math.min(GeneratorImport.DEFAULT_COUNT, GeneratorItemReader.DEFAULT_KEY_RANGE.getMax()),
				commands.dbsize());
	}

}
