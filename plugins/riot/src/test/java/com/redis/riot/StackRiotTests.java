package com.redis.riot;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.lettucemod.RedisModulesUtils;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.search.SuggetOptions;
import com.redis.lettucemod.timeseries.MRangeOptions;
import com.redis.lettucemod.timeseries.RangeResult;
import com.redis.lettucemod.timeseries.TimeRange;
import com.redis.riot.core.Expression;
import com.redis.riot.core.QuietMapAccessor;
import com.redis.riot.file.xml.XmlItemReader;
import com.redis.riot.file.xml.XmlItemReaderBuilder;
import com.redis.riot.file.xml.XmlObjectReader;
import com.redis.spring.batch.item.redis.common.DataType;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.gen.GeneratorItemReader;
import com.redis.spring.batch.item.redis.gen.MapOptions;
import com.redis.spring.batch.item.redis.reader.DefaultKeyComparator;
import com.redis.spring.batch.item.redis.reader.KeyComparison;
import com.redis.spring.batch.item.redis.reader.KeyComparison.Status;
import com.redis.spring.batch.item.redis.reader.KeyComparisonItemReader;
import com.redis.spring.batch.test.KeyspaceComparison;
import com.redis.testcontainers.RedisStackContainer;

import io.lettuce.core.GeoArgs;
import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.cluster.SlotHash;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.ParseResult;

class StackRiotTests extends RiotTests {

	public static final int BEER_CSV_COUNT = 2410;
	public static final int BEER_JSON_COUNT = 216;

	private static final RedisStackContainer source = RedisContainerFactory.stack();
	private static final RedisStackContainer target = RedisContainerFactory.stack();

	private static Path tempDir;

	@BeforeAll
	public void setupFiles() throws IOException {
		tempDir = Files.createTempDirectory(getClass().getName());
	}

	protected Path tempFile(String filename) throws IOException {
		Path path = tempDir.resolve(filename);
		if (Files.exists(path)) {
			Files.delete(path);
		}
		return path;
	}

	@Override
	protected RedisStackContainer getRedisServer() {
		return source;
	}

	@Override
	protected RedisStackContainer getTargetRedisServer() {
		return target;
	}

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

	@SuppressWarnings("rawtypes")
	@Test
	void fileImportJsonDump(TestInfo info) throws Exception {
		List<? extends KeyValue> records = exportToJsonFile(info);
		redisCommands.flushall();
		execute(info, "file-import-json", this::executeFileDumpImport);
		awaitUntil(() -> records.size() == Math.toIntExact(redisCommands.dbsize()));
	}

	@SuppressWarnings("rawtypes")
	@Test
	void fileExportJSON(TestInfo info) throws Exception {
		List<? extends KeyValue> records = exportToJsonFile(info);
		Assertions.assertEquals(redisCommands.dbsize(), records.size());
	}

	@SuppressWarnings("rawtypes")
	private List<? extends KeyValue> exportToJsonFile(TestInfo info) throws Exception {
		String filename = "file-export-json";
		Path file = tempFile("redis.json");
		generate(info, generator(73));
		execute(info, filename, r -> executeFileDumpExport(r, info));
		JsonItemReaderBuilder<KeyValue> builder = new JsonItemReaderBuilder<>();
		builder.name("json-reader");
		builder.resource(new FileSystemResource(file));
		JacksonJsonObjectReader<KeyValue> objectReader = new JacksonJsonObjectReader<>(KeyValue.class);
		objectReader.setMapper(new ObjectMapper());
		builder.jsonObjectReader(objectReader);
		JsonItemReader<KeyValue> reader = builder.build();
		return readAll(info, reader);
	}

	private int executeFileDumpImport(ParseResult parseResult) {
		FileImport command = command(parseResult);
		command.setFiles(command.getFiles().stream().map(this::replace).collect(Collectors.toList()));
		return ExitCode.OK;
	}

	private int executeFileDumpExport(ParseResult parseResult, TestInfo info) {
		FileExport command = command(parseResult);
		command.setJobName(name(info));
		command.setFile(replace(command.getFile()));
		return ExitCode.OK;
	}

	private String replace(String file) {
		return file.replace("/tmp", tempDir.toString());
	}

	@SuppressWarnings("rawtypes")
	@Test
	@Disabled("Needs update")
	void fileExportJSONGz(TestInfo info) throws Exception {
		Path file = tempFile("beers.json.gz");
		execute(info, "file-import-json-hset");
		execute(info, "file-export-json-gz-hset", r -> executeFileDumpExport(r, info));
		JsonItemReaderBuilder<Map> builder = new JsonItemReaderBuilder<>();
		builder.name("json-reader");
		FileSystemResource resource = new FileSystemResource(file);
		builder.resource(
				new InputStreamResource(new GZIPInputStream(resource.getInputStream()), resource.getDescription()));
		JacksonJsonObjectReader<Map> objectReader = new JacksonJsonObjectReader<>(Map.class);
		objectReader.setMapper(new ObjectMapper());
		builder.jsonObjectReader(objectReader);
		JsonItemReader<Map> reader = builder.build();
		Assertions.assertEquals(keyCount("beer:*"), readAll(info, reader).size());
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void fileExportXml(TestInfo info) throws Exception {
		String filename = "file-export-xml";
		generate(info, generator(73));
		Path file = tempFile("redis.xml");
		execute(info, filename, r -> executeFileDumpExport(r, info));
		XmlItemReaderBuilder<KeyValue> builder = new XmlItemReaderBuilder<>();
		builder.name("xml-reader");
		builder.resource(new FileSystemResource(file));
		XmlObjectReader<KeyValue> xmlObjectReader = new XmlObjectReader<>(KeyValue.class);
		xmlObjectReader.setMapper(new XmlMapper());
		builder.xmlObjectReader(xmlObjectReader);
		XmlItemReader<KeyValue<String>> reader = (XmlItemReader) builder.build();
		List<? extends KeyValue<String>> records = readAll(info, reader);
		Assertions.assertEquals(redisCommands.dbsize(), records.size());
		for (KeyValue<String> record : records) {
			DataType type = KeyValue.type(record);
			if (type == null) {
				continue;
			}
			switch (type) {
			case HASH:
				Assertions.assertEquals(record.getValue(), redisCommands.hgetall(record.getKey()));
				break;
			case STRING:
				Assertions.assertEquals(record.getValue(), redisCommands.get(record.getKey()));
				break;
			default:
				break;
			}
		}
	}

	@Test
	void fileImportFW(TestInfo info) throws Exception {
		testImport(info, "file-import-fw", "account:*", 5);
		Map<String, String> account101 = redisCommands.hgetall("account:101");
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
		testImport(info, "file-import-filetype", "sample:*", 3);
	}

	@Test
	void fileImportExclude(TestInfo info) throws Exception {
		execute(info, "file-import-exclude");
		Map<String, String> beer1036 = redisCommands.hgetall("beer:1036");
		Assertions.assertEquals("Lower De Boom", name(beer1036));
		Assertions.assertEquals("American Barleywine", style(beer1036));
		Assertions.assertEquals("368", beer1036.get("brewery_id"));
		Assertions.assertFalse(beer1036.containsKey("row"));
		Assertions.assertFalse(beer1036.containsKey("ibu"));
	}

	@Test
	void fileImportInclude(TestInfo info) throws Exception {
		execute(info, "file-import-include");
		Map<String, String> beer1036 = redisCommands.hgetall("beer:1036");
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
		Map<String, String> airport1 = redisCommands.hgetall("airport:1");
		Assertions.assertEquals("Pacific", airport1.get("region"));
		Assertions.assertEquals("Port_Moresby", airport1.get("city"));
	}

	@Test
	void fileImportGlob(TestInfo info) throws Exception {
		execute(info, "file-import-glob", this::executeImportGlob);
		Assertions.assertEquals(BEER_CSV_COUNT, keyCount("beer:*"));
	}

	private int executeImportGlob(ParseResult parseResult) {
		FileImport command = command(parseResult);
		try {
			Path dir = Files.createTempDirectory("import-glob");
			FileCopyUtils.copy(getClass().getClassLoader().getResourceAsStream("files/beers1.csv"),
					Files.newOutputStream(dir.resolve("beers1.csv")));
			FileCopyUtils.copy(getClass().getClassLoader().getResourceAsStream("files/beers2.csv"),
					Files.newOutputStream(dir.resolve("beers2.csv")));
			File file = new File(command.getFiles().get(0));
			command.setFiles(dir.resolve(file.getName()).toString());
		} catch (IOException e) {
			throw new RuntimeException("Could not configure import-glob", e);
		}
		return ExitCode.OK;
	}

	@Test
	void fileImportGeoadd(TestInfo info) throws Exception {
		execute(info, "file-import-geoadd");
		Set<String> results = redisCommands.georadius("airportgeo", -21, 64, 200, GeoArgs.Unit.mi);
		Assertions.assertTrue(results.contains("18"));
		Assertions.assertTrue(results.contains("19"));
		Assertions.assertTrue(results.contains("11"));
	}

	@Test
	void fileImportGeoProcessor(TestInfo info) throws Exception {
		execute(info, "file-import-geo-processor");
		Map<String, String> airport3469 = redisCommands.hgetall("airport:18");
		Assertions.assertEquals("-21.9405994415,64.1299972534", airport3469.get("location"));
	}

	@Test
	void fileImportProcess(TestInfo info) throws Exception {
		testImport(info, "file-import-process", "event:*", 568);
		Map<String, String> event = redisCommands.hgetall("event:248206");
		Instant date = Instant.ofEpochMilli(Long.parseLong(event.get("EpochStart")));
		Assertions.assertTrue(date.isBefore(Instant.now()));
	}

	@Test
	void fileImportProcessFaker(TestInfo info) throws Exception {
		testImport(info, "file-import-process-faker", "beer:*", BEER_CSV_COUNT);
		Map<String, String> beer = redisCommands.hgetall(redisCommands.randomkey());
		Assertions.assertTrue(StringUtils.hasLength(beer.get("fakeid")));
	}

	@Test
	void fileImportProcessVar(TestInfo info) throws Exception {
		testImport(info, "file-import-process-var", "event:*", 568);
		Map<String, String> event = redisCommands.hgetall("event:248206");
		int randomInt = Integer.parseInt(event.get("randomInt"));
		Assertions.assertTrue(randomInt >= 0 && randomInt < 100);
	}

	@Test
	void fileImportProcessElvis(TestInfo info) throws Exception {
		testImport(info, "file-import-process-elvis", "beer:*", BEER_CSV_COUNT);
		Map<String, String> beer1436 = redisCommands.hgetall("beer:1436");
		Assertions.assertEquals("10", beer1436.get("ibu"));
	}

	@Test
	void fileImportHsetSadd(TestInfo info) throws Exception {
		execute(info, "file-import-hset-sadd");
		List<String> beers = redisCommands.keys("beer:*");
		Assertions.assertEquals(BEER_CSV_COUNT, beers.size());
		for (String beer : beers) {
			Map<String, String> hash = redisCommands.hgetall(beer);
			Assertions.assertTrue(hash.containsKey("name"));
			Assertions.assertTrue(hash.containsKey("brewery_id"));
		}
		Set<String> breweries = redisCommands.smembers("breweries");
		Assertions.assertEquals(558, breweries.size());
	}

	@Test
	void fileImportHsetExpire(TestInfo info) throws Exception {
		execute(info, "file-import-hset-expire");
		List<String> beers = redisCommands.keys("beer:*");
		Assertions.assertEquals(BEER_CSV_COUNT, beers.size());
		for (String beer : beers) {
			Map<String, String> hash = redisCommands.hgetall(beer);
			Assertions.assertTrue(hash.containsKey("name"));
			Assertions.assertTrue(hash.containsKey("brewery_id"));
			Assertions.assertEquals(3600, redisCommands.ttl(beer), 3);
		}
	}

	@Test
	void fileImportHsetExpireField(TestInfo info) throws Exception {
		execute(info, "file-import-hset-expire-abs");
		List<String> beers = redisCommands.keys("beer:*");
		Assertions.assertEquals(BEER_CSV_COUNT, beers.size());
		for (String beer : beers) {
			Map<String, String> hash = redisCommands.hgetall(beer);
			Assertions.assertTrue(hash.containsKey("name"));
			Assertions.assertTrue(hash.containsKey("brewery_id"));
			Assertions.assertEquals(10, redisCommands.ttl(beer), 5);
		}
	}

	@Test
	void fileImportBad(TestInfo info) throws Exception {
		Assertions.assertEquals(0, execute(info, "file-import-bad"));
	}

	@Test
	void fileImportGCS(TestInfo info) throws Exception {
		testImport(info, "file-import-gcs", "beer:*", 4432);
		Map<String, String> beer1 = redisCommands.hgetall("beer:1");
		Assertions.assertEquals("Hocus Pocus", name(beer1));
	}

	@Test
	void fileImportS3(TestInfo info) throws Exception {
		testImport(info, "file-import-s3", "beer:*", 4432);
		Map<String, String> beer1 = redisCommands.hgetall("beer:1");
		Assertions.assertEquals("Hocus Pocus", name(beer1));
	}

	@Test
	@Disabled("Needs update")
	void fileImportJSONElastic(TestInfo info) throws Exception {
		execute(info, "file-import-json-elastic-hset");
		Assertions.assertEquals(2, keyCount("estest:*"));
		Map<String, String> doc1 = redisCommands.hgetall("estest:doc1");
		Assertions.assertEquals("ruan", doc1.get("_source.name"));
		Assertions.assertEquals("3", doc1.get("_source.articles[1]"));
	}

	@Test
	void fileImportJSON(TestInfo info) throws Exception {
		testImport(info, "file-import-json-hset", "beer:*", BEER_JSON_COUNT);
		Map<String, String> beer1 = redisCommands.hgetall("beer:1");
		Assertions.assertEquals("Hocus Pocus", beer1.get("name"));
	}

	@Test
	void fileImportXML(TestInfo info) throws Exception {
		testImport(info, "file-import-xml", "trade:*", 3);
		Map<String, String> trade1 = redisCommands.hgetall("trade:1");
		Assertions.assertEquals("XYZ0001", trade1.get("isin"));
	}

	@Test
	void fileImportJSONGzip(TestInfo info) throws Exception {
		testImport(info, "file-import-json-gz-hset", "beer:*", BEER_JSON_COUNT);
	}

	@Test
	void fileImportSugadd(TestInfo info) throws Exception {
		assertExecutionSuccessful(execute(info, "file-import-sugadd"));
		List<Suggestion<String>> suggestions = redisCommands.ftSugget("names", "Bea",
				SuggetOptions.builder().withPayloads(true).build());
		Assertions.assertEquals(5, suggestions.size());
		Assertions.assertEquals("American Blonde Ale", suggestions.get(0).getPayload());
	}

	@Test
	void fileImportElasticJSON(TestInfo info) throws Exception {
		assertExecutionSuccessful(execute(info, "file-import-json-elastic-jsonset"));
		Assertions.assertEquals(2, keyCount("elastic:*"));
		ObjectMapper mapper = new ObjectMapper();
		String doc1 = redisCommands.jsonGet("elastic:doc1");
		String expected = "{\"_index\":\"test-index\",\"_type\":\"docs\",\"_id\":\"doc1\",\"_score\":1,\"_source\":{\"name\":\"ruan\",\"age\":30,\"articles\":[\"1\",\"3\"]}}";
		Assertions.assertEquals(mapper.readTree(expected), mapper.readTree(doc1));
	}

	@Test
	void fakerImportHset(TestInfo info) throws Exception {
		testImport(info, "faker-hset", "person:*", 1000);
		Map<String, String> person = redisCommands.hgetall(redisCommands.randomkey());
		Assertions.assertTrue(person.containsKey("firstName"));
		Assertions.assertTrue(person.containsKey("lastName"));
		Assertions.assertTrue(person.containsKey("address"));
	}

	@Test
	void fakerImportSadd(TestInfo info) throws Exception {
		execute(info, "faker-sadd");
		Set<String> names = redisCommands.smembers("got:characters");
		Assertions.assertTrue(names.size() > 10);
		for (String name : names) {
			Assertions.assertFalse(name.isEmpty());
		}
	}

	@Test
	void fakerImportZadd(TestInfo info) throws Exception {
		execute(info, "faker-zadd");
		Assertions.assertTrue(keyCount("leases:*") > 100);
		String key;
		do {
			key = redisCommands.randomkey();
		} while (key == null);
		Assertions.assertTrue(redisCommands.zcard(key) > 0);
	}

	@Test
	void fakerImportXadd(TestInfo info) throws Exception {
		execute(info, "faker-xadd");
		List<StreamMessage<String, String>> messages = redisCommands.xrange("teststream:1", Range.unbounded());
		Assertions.assertTrue(messages.size() > 0);
	}

	@Test
	void fakerImportTsAddWithOptions(TestInfo info) throws Exception {
		execute(info, "faker-tsadd-options");
		List<RangeResult<String, String>> results = redisCommands.tsMrange(TimeRange.unbounded(),
				MRangeOptions.<String, String>filters("character1=Einstein").build());
		Assertions.assertFalse(results.isEmpty());
	}

	@Test
	void generateTypes(TestInfo info) throws Exception {
		execute(info, "generate");
		Assertions.assertEquals(Math.min(GenerateArgs.DEFAULT_COUNT, GeneratorItemReader.DEFAULT_KEY_RANGE.getMax()),
				redisCommands.dbsize());
	}

	@Test
	void generateJsonIndex(TestInfo info) throws Exception {
		execute(info, "generate-json-index");
		int keyCount = Math.min(GenerateArgs.DEFAULT_COUNT, GeneratorItemReader.DEFAULT_KEY_RANGE.getMax());
		Assertions.assertEquals(keyCount, redisCommands.dbsize());
		IndexInfo indexInfo = RedisModulesUtils.indexInfo(redisCommands.ftInfo("jsonIdx"));
		List<Field<String>> expectedFields = new ArrayList<>();
		for (int index = 1; index <= MapOptions.DEFAULT_FIELD_COUNT.getMax(); index++) {
			expectedFields.add(Field.tag("$.field" + index).as("field" + index).build());
		}
		Assertions.assertEquals(expectedFields, indexInfo.getFields());
		Assertions.assertEquals(keyCount, indexInfo.getNumDocs());
	}

	@Test
	void generateHashIndex(TestInfo info) throws Exception {
		execute(info, "generate-hash-index");
		int keyCount = Math.min(GenerateArgs.DEFAULT_COUNT, GeneratorItemReader.DEFAULT_KEY_RANGE.getMax());
		Assertions.assertEquals(keyCount, redisCommands.dbsize());
		IndexInfo indexInfo = RedisModulesUtils.indexInfo(redisCommands.ftInfo("hashIdx"));
		List<Field<String>> expectedFields = new ArrayList<>();
		for (int index = 1; index <= MapOptions.DEFAULT_FIELD_COUNT.getMax(); index++) {
			expectedFields.add(Field.tag("field" + index).as("field" + index).separator(',').build());
		}
		Assertions.assertEquals(expectedFields, indexInfo.getFields());
		Assertions.assertEquals(keyCount, indexInfo.getNumDocs());
	}

	@Test
	void replicateKeyExclude(TestInfo info) throws Throwable {
		String filename = "replicate-key-exclude";
		int goodCount = 200;
		GeneratorItemReader gen = generator(goodCount, DataType.HASH);
		generate(info, gen);
		int badCount = 100;
		GeneratorItemReader generator2 = generator(badCount, DataType.HASH);
		generator2.setKeyspace("bad");
		generate(testInfo(info, "2"), generator2);
		Assertions.assertEquals(badCount, keyCount("bad:*"));
		execute(info, filename);
		Assertions.assertEquals(goodCount, targetRedisCommands.keys("gen:*").size());
	}

	@Test
	void replicateLiveKeyExclude(TestInfo info) throws Throwable {
		String filename = "replicate-live-key-exclude";
		int goodCount = 200;
		int badCount = 100;
		enableKeyspaceNotifications();
		generateAsync(testInfo(info, "gen-1"), generator(goodCount, DataType.HASH));
		GeneratorItemReader generator2 = generator(badCount, DataType.HASH);
		generator2.setKeyspace("bad");
		generateAsync(testInfo(info, "gen-2"), generator2);
		execute(info, filename);
		awaitUntil(() -> redisCommands.pubsubNumpat() == 0);
		Assertions.assertEquals(badCount, keyCount("bad:*"));
		Assertions.assertEquals(0, targetRedisCommands.keys("bad:*").size());
		Assertions.assertEquals(goodCount, targetRedisCommands.keys("gen:*").size());
	}

	@Test
	void replicateLiveOnlyStruct(TestInfo info) throws Exception {
		DataType[] types = new DataType[] { DataType.HASH, DataType.STRING };
		enableKeyspaceNotifications();
		GeneratorItemReader generator = generator(3500, types);
		generator.setCurrentItemCount(3001);
		generateAsync(testInfo(info, "async"), generator);
		execute(info, "replicate-live-only-struct");
		KeyspaceComparison<String> comparison = compare(info);
		Assertions.assertFalse(comparison.getAll().isEmpty());
		Assertions.assertEquals(Collections.emptyList(), comparison.mismatches());
	}

	@Test
	void replication(TestInfo info) throws Throwable {
		generate(info, generator(73));
		Assertions.assertTrue(redisCommands.dbsize() > 0);
		Replicate replication = new Replicate();
		execute(replication, info);
		KeyspaceComparison<String> comparison = compare(info);
		Assertions.assertFalse(comparison.getAll().isEmpty());
		Assertions.assertEquals(Collections.emptyList(), comparison.mismatches());
	}

	@Test
	void replicate(TestInfo info) throws Throwable {
		String filename = "replicate";
		generate(info, generator(73));
		Assertions.assertTrue(redisCommands.dbsize() > 0);
		execute(info, filename);
		assertCompare(info);
	}

	@Test
	void replicateNoStreamId(TestInfo info) throws Throwable {
		String filename = "replicate-no-stream-id";
		generate(info, generator(73));
		Assertions.assertTrue(redisCommands.dbsize() > 0);
		execute(info, filename);
		assertDbNotEmpty(redisCommands);
		KeyComparisonItemReader<String, String> reader = comparisonReader(info);
		((DefaultKeyComparator<String, String>) reader.getComparator()).setIgnoreStreamMessageId(true);
		List<? extends KeyComparison<String>> comparisons = readAll(info, reader);
		KeyspaceComparison<String> comparison = new KeyspaceComparison<>(comparisons);
		Assertions.assertFalse(comparison.getAll().isEmpty());
		Assertions.assertEquals(Collections.emptyList(), comparison.mismatches());
	}

	@Test
	void replicateNoStreamIdPrune(TestInfo info) throws Throwable {
		String filename = "replicate-no-stream-id-prune";
		generate(info, generator(73));
		String emptyStream = "stream:empty";
		redisCommands.xadd(emptyStream, Map.of("field", "value"));
		redisCommands.xtrim(emptyStream, 0);
		Assertions.assertTrue(redisCommands.dbsize() > 0);
		execute(info, filename);
		assertDbNotEmpty(redisCommands);
		KeyComparisonItemReader<String, String> reader = comparisonReader(info);
		((DefaultKeyComparator<String, String>) reader.getComparator()).setIgnoreStreamMessageId(true);
		List<? extends KeyComparison<String>> comparisons = readAll(info, reader);
		KeyspaceComparison<String> comparison = new KeyspaceComparison<>(comparisons);
		Assertions.assertFalse(comparison.getAll().isEmpty());
		KeyComparison<String> missing = comparison.mismatches().get(0);
		Assertions.assertEquals(Status.MISSING, missing.getStatus());
		Assertions.assertEquals(emptyStream, missing.getSource().getKey());
	}

	@Test
	void replicateDryRun(TestInfo info) throws Throwable {
		String filename = "replicate-dry-run";
		generate(info, generator(73));
		Assertions.assertTrue(redisCommands.dbsize() > 0);
		execute(info, filename);
		Assertions.assertEquals(0, targetRedisCommands.dbsize());
	}

	@Test
	void replicateHyperloglog(TestInfo info) throws Throwable {
		String key = "crawled:20171124";
		String value = "http://www.google.com/";
		redisCommands.pfadd(key, value);
		Assertions.assertEquals(0, execute(info, "replicate-hll"));
		assertCompare(info);
	}

	@Test
	void replicateKeyProcessor(TestInfo info) throws Throwable {
		String filename = "replicate-key-processor";
		GeneratorItemReader gen = generator(1, DataType.HASH);
		generate(info, gen);
		Long sourceSize = redisCommands.dbsize();
		Assertions.assertTrue(sourceSize > 0);
		execute(info, filename);
		Assertions.assertEquals(sourceSize, targetRedisCommands.dbsize());
		Assertions.assertEquals(redisCommands.hgetall("gen:1"), targetRedisCommands.hgetall("prefix:gen:1"));
	}

	@Test
	void replicateLiveStruct(TestInfo info) throws Exception {
		runLiveReplication(info, "replicate-live-struct");
	}

	@Test
	void replicateLiveThreads(TestInfo info) throws Exception {
		runLiveReplication(info, "replicate-live-threads");
	}

	@Test
	void replicateLive(TestInfo info) throws Exception {
		runLiveReplication(info, "replicate-live");
	}

	@Test
	void replicateLiveReadThreads(TestInfo info) throws Exception {
		runLiveReplication(info, "replicate-live-read-threads");
	}

	@Test
	void replicateLiveKeySlot(TestInfo info) throws Exception {
		String filename = "replicate-live-keyslot";
		enableKeyspaceNotifications();
		int count = 300;
		GeneratorItemReader generator = generator(count);
		generateAsync(info, generator);
		execute(info, filename);
		List<String> keys = targetRedisCommands.keys("*");
		Assertions.assertEquals(148, keys.size());
		for (String key : keys) {
			int slot = SlotHash.getSlot(key);
			Assertions.assertTrue(slot >= 0 && slot <= 8000);
		}
	}

	@Test
	void replicateStruct(TestInfo info) throws Throwable {
		String filename = "replicate-struct";
		GeneratorItemReader generator = generator(12000);
		generate(info, generator);
		Assertions.assertTrue(redisCommands.dbsize() > 0);
		execute(info, filename);
	}

	@Test
	void compareKeyProcessor(TestInfo info) throws Throwable {
		GeneratorItemReader gen = generator(1, DataType.HASH);
		generate(info, gen);
		Long sourceSize = redisCommands.dbsize();
		Assertions.assertTrue(sourceSize > 0);
		execute(testInfo(info, "replicate"), "replicate-key-processor-compare-none");
		Assertions.assertEquals(sourceSize, targetRedisCommands.dbsize());
		Assertions.assertEquals(redisCommands.hgetall("gen:1"), targetRedisCommands.hgetall("prefix:gen:1"));
		execute(info, "compare-key-processor");
	}

	@Test
	void keyProcessor(TestInfo info) throws Throwable {
		String key1 = "key1";
		String value1 = "value1";
		redisCommands.set(key1, value1);
		Replicate command = new Replicate();
		command.setStruct(true);
		command.getProcessorArgs().setKeyExpression(Expression.parseTemplate("#{type}:#{key}"));
		execute(command, info);
		Assertions.assertEquals(value1, targetRedisCommands.get("string:" + key1));
	}

	@Test
	void keyProcessorWithDate(TestInfo info) throws Throwable {
		String key1 = "key1";
		String value1 = "value1";
		redisCommands.set(key1, value1);
		Replicate replication = new Replicate();
		replication.getProcessorArgs().setKeyExpression(Expression
				.parseTemplate(String.format("#{#date.parse('%s').getTime()}:#{key}", "2010-05-10T00:00:00.000+0000")));
		execute(replication, info);
		Assertions.assertEquals(value1, targetRedisCommands.get("1273449600000:" + key1));
	}

	@Test
	void testMapProcessor() throws Exception {
		Map<String, Expression> expressions = new LinkedHashMap<>();
		expressions.put("field1", Expression.parse("'test:1'"));
		ImportProcessorArgs args = new ImportProcessorArgs();
		args.setExpressions(expressions);
		ItemProcessor<Map<String, Object>, Map<String, Object>> processor = AbstractImportCommand
				.processor(evaluationContext(), args);
		Map<String, Object> map = processor.process(new HashMap<>());
		Assertions.assertEquals("test:1", map.get("field1"));
		// Assertions.assertEquals("1", map.get("id"));
	}

	@Test
	void processor() throws Exception {
		Map<String, Expression> expressions = new LinkedHashMap<>();
		expressions.put("field1", Expression.parse("'value1'"));
		expressions.put("field2", Expression.parse("field1"));
		expressions.put("field3", Expression.parse("1"));
		expressions.put("field4", Expression.parse("2"));
		expressions.put("field5", Expression.parse("field3+field4"));
		ImportProcessorArgs args = new ImportProcessorArgs();
		args.setExpressions(expressions);
		ItemProcessor<Map<String, Object>, Map<String, Object>> processor = AbstractImportCommand
				.processor(evaluationContext(), args);
		for (int index = 0; index < 10; index++) {
			Map<String, Object> result = processor.process(new HashMap<>());
			assertEquals(5, result.size());
			assertEquals("value1", result.get("field1"));
			assertEquals("value1", result.get("field2"));
			assertEquals(3, result.get("field5"));
		}
	}

	private EvaluationContext evaluationContext() {
		StandardEvaluationContext context = new StandardEvaluationContext();
		context.addPropertyAccessor(new QuietMapAccessor());
		return context;
	}

	@Test
	void processorFilter() throws Exception {
		ImportProcessorArgs args = new ImportProcessorArgs();
		args.setFilter(Expression.parse("index<10"));
		ItemProcessor<Map<String, Object>, Map<String, Object>> processor = AbstractImportCommand
				.processor(evaluationContext(), args);
		for (int index = 0; index < 100; index++) {
			Map<String, Object> map = new HashMap<>();
			map.put("index", index);
			Map<String, Object> result = processor.process(map);
			if (index < 10) {
				Assertions.assertNotNull(result);
			} else {
				Assertions.assertNull(result);
			}
		}
	}

}
