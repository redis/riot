package com.redis.riot.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.util.FileCopyUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.riot.AbstractRiotIntegrationTests;
import com.redis.riot.ProgressStyle;
import com.redis.riot.file.resource.XmlItemReader;
import com.redis.riot.file.resource.XmlItemReaderBuilder;
import com.redis.riot.file.resource.XmlObjectReader;
import com.redis.riot.redis.HsetCommand;
import com.redis.spring.batch.common.DataStructure;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

import io.lettuce.core.GeoArgs;
import io.lettuce.core.api.sync.RedisGeoCommands;
import io.lettuce.core.api.sync.RedisHashCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.api.sync.RedisSetCommands;
import picocli.CommandLine;

@SuppressWarnings("unchecked")
class RiotFileIntegrationTests extends AbstractRiotIntegrationTests {

	public static final String BEERS_JSON_URL = "https://storage.googleapis.com/jrx/beers.json";
	public static final int BEER_CSV_COUNT = 2410;
	public static final int BEER_JSON_COUNT = 216;

	private static Path tempDir;

	@BeforeAll
	public void setupAll() throws IOException {
		tempDir = Files.createTempDirectory(RiotFileIntegrationTests.class.getName());
	}

	private String replace(String file) {
		return file.replace("/tmp", tempDir.toString());
	}

	protected Path tempFile(String filename) throws IOException {
		Path path = tempDir.resolve(filename);
		if (Files.exists(path)) {
			Files.delete(path);
		}
		return path;
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

	protected <T> List<T> readAll(AbstractItemCountingItemStreamItemReader<T> reader) throws Exception {
		reader.open(new ExecutionContext());
		List<T> records = new ArrayList<>();
		T record;
		while ((record = reader.read()) != null) {
			records.add(record);
		}
		reader.close();
		return records;
	}

	@Override
	protected RiotFile app() {
		return new RiotFile();
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importFW(RedisTestContext redis) throws Exception {
		execute("import-fw", redis);
		RedisKeyCommands<String, String> sync = redis.sync();
		List<String> keys = sync.keys("account:*");
		Awaitility.await().until(() -> keys.size() == 5);
		RedisHashCommands<String, String> hash = redis.sync();
		Map<String, String> account101 = hash.hgetall("account:101");
		// Account LastName FirstName Balance CreditLimit AccountCreated Rating
		// 101 Reeves Keanu 9315.45 10000.00 1/17/1998 A
		Assertions.assertEquals("Reeves", account101.get("LastName"));
		Assertions.assertEquals("Keanu", account101.get("FirstName"));
		Assertions.assertEquals("A", account101.get("Rating"));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importCSV(RedisTestContext redis) throws Exception {
		execute("import-csv", redis);
		RedisKeyCommands<String, String> sync = redis.sync();
		List<String> keys = sync.keys("beer:*");
		Assertions.assertEquals(BEER_CSV_COUNT, keys.size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importPSV(RedisTestContext redis) throws Exception {
		execute("import-psv", redis);
		RedisKeyCommands<String, String> sync = redis.sync();
		List<String> keys = sync.keys("sample:*");
		Assertions.assertEquals(3, keys.size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importTSV(RedisTestContext redis) throws Exception {
		execute("import-tsv", redis);
		RedisKeyCommands<String, String> sync = redis.sync();
		List<String> keys = sync.keys("sample:*");
		Assertions.assertEquals(4, keys.size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importType(RedisTestContext redis) throws Exception {
		execute("import-type", redis);
		RedisKeyCommands<String, String> sync = redis.sync();
		List<String> keys = sync.keys("sample:*");
		Assertions.assertEquals(3, keys.size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importExclude(RedisTestContext redis) throws Exception {
		execute("import-exclude", redis);
		RedisHashCommands<String, String> sync = redis.sync();
		Map<String, String> beer1036 = sync.hgetall("beer:1036");
		Assertions.assertEquals("Lower De Boom", name(beer1036));
		Assertions.assertEquals("American Barleywine", style(beer1036));
		Assertions.assertEquals("368", beer1036.get("brewery_id"));
		Assertions.assertFalse(beer1036.containsKey("row"));
		Assertions.assertFalse(beer1036.containsKey("ibu"));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importInclude(RedisTestContext redis) throws Exception {
		execute("import-include", redis);
		RedisHashCommands<String, String> sync = redis.sync();
		Map<String, String> beer1036 = sync.hgetall("beer:1036");
		Assertions.assertEquals(3, beer1036.size());
		Assertions.assertEquals("Lower De Boom", name(beer1036));
		Assertions.assertEquals("American Barleywine", style(beer1036));
		Assertions.assertEquals(0.099, abv(beer1036));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importFilter(RedisTestContext redis) throws Exception {
		execute("import-filter", redis);
		RedisKeyCommands<String, String> sync = redis.sync();
		List<String> keys = sync.keys("beer:*");
		Assertions.assertEquals(424, keys.size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importRegex(RedisTestContext redis) throws Exception {
		execute("import-regex", redis);
		RedisHashCommands<String, String> sync = redis.sync();
		Map<String, String> airport1 = sync.hgetall("airport:1");
		Assertions.assertEquals("Pacific", airport1.get("region"));
		Assertions.assertEquals("Port_Moresby", airport1.get("city"));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importGlob(RedisTestContext redis) throws Exception {
		execute("import-glob", redis, this::configureImportGlob);
		RedisKeyCommands<String, String> sync = redis.sync();
		List<String> keys = sync.keys("beer:*");
		Assertions.assertEquals(BEER_CSV_COUNT, keys.size());
	}

	private void configureImportGlob(CommandLine.ParseResult parseResult) {
		FileImportCommand command = parseResult.subcommand().commandSpec().parent().commandLine().getCommand();
		try {
			Path dir = Files.createTempDirectory("import-glob");
			FileCopyUtils.copy(getClass().getClassLoader().getResourceAsStream("files/beers1.csv"),
					Files.newOutputStream(dir.resolve("beers1.csv")));
			FileCopyUtils.copy(getClass().getClassLoader().getResourceAsStream("files/beers2.csv"),
					Files.newOutputStream(dir.resolve("beers2.csv")));
			File file = new File(command.getOptions().getFiles().get(0));
			command.getOptions().setFiles(Arrays.asList(dir.resolve(file.getName()).toString()));
		} catch (IOException e) {
			throw new RuntimeException("Could not configure import-glob", e);
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importGeoadd(RedisTestContext redis) throws Exception {
		execute("import-geoadd", redis);
		RedisGeoCommands<String, String> sync = redis.sync();
		Set<String> results = sync.georadius("airportgeo", -21, 64, 200, GeoArgs.Unit.mi);
		Assertions.assertTrue(results.contains("18"));
		Assertions.assertTrue(results.contains("19"));
		Assertions.assertTrue(results.contains("11"));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importGeoProcessor(RedisTestContext redis) throws Exception {
		execute("import-geo-processor", redis);
		RedisHashCommands<String, String> sync = redis.sync();
		Map<String, String> airport3469 = sync.hgetall("airport:18");
		Assertions.assertEquals("-21.9405994415,64.1299972534", airport3469.get("location"));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importProcess(RedisTestContext redis) throws Exception {
		execute("import-process", redis);
		RedisKeyCommands<String, String> sync = redis.sync();
		List<String> keys = sync.keys("event:*");
		Assertions.assertEquals(568, keys.size());
		RedisHashCommands<String, String> hash = redis.sync();
		Map<String, String> event = hash.hgetall("event:248206");
		Instant date = Instant.ofEpochMilli(Long.parseLong(event.get("EpochStart")));
		Assertions.assertTrue(date.isBefore(Instant.now()));
		long index = Long.parseLong(event.get("index"));
		Assertions.assertTrue(index > 0);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importProcessElvis(RedisTestContext redis) throws Exception {
		execute("import-process-elvis", redis);
		RedisKeyCommands<String, String> sync = redis.sync();
		List<String> keys = sync.keys("beer:*");
		Assertions.assertEquals(BEER_CSV_COUNT, keys.size());
		Map<String, String> beer1436 = ((RedisHashCommands<String, String>) sync).hgetall("beer:1436");
		Assertions.assertEquals("10", beer1436.get("ibu"));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importMultiCommands(RedisTestContext redis) throws Exception {
		execute("import-multi-commands", redis);
		RedisKeyCommands<String, String> sync = redis.sync();
		List<String> beers = sync.keys("beer:*");
		Assertions.assertEquals(BEER_CSV_COUNT, beers.size());
		for (String beer : beers) {
			Map<String, String> hash = ((RedisHashCommands<String, String>) sync).hgetall(beer);
			Assertions.assertTrue(hash.containsKey("name"));
			Assertions.assertTrue(hash.containsKey("brewery_id"));
		}
		RedisSetCommands<String, String> set = redis.sync();
		Set<String> breweries = set.smembers("breweries");
		Assertions.assertEquals(558, breweries.size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importBad(RedisTestContext redis) throws Exception {
		Assertions.assertEquals(0, execute("import-bad", redis));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importGCS(RedisTestContext redis) throws Exception {
		execute("import-gcs", redis);
		RedisKeyCommands<String, String> sync = redis.sync();
		List<String> keys = sync.keys("beer:*");
		Assertions.assertEquals(4432, keys.size());
		Map<String, String> beer1 = ((RedisHashCommands<String, String>) sync).hgetall("beer:1");
		Assertions.assertEquals("Hocus Pocus", name(beer1));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importS3(RedisTestContext redis) throws Exception {
		execute("import-s3", redis);
		RedisKeyCommands<String, String> sync = redis.sync();
		List<String> keys = sync.keys("beer:*");
		Assertions.assertEquals(4432, keys.size());
		Map<String, String> beer1 = ((RedisHashCommands<String, String>) sync).hgetall("beer:1");
		Assertions.assertEquals("Hocus Pocus", name(beer1));
	}

	@SuppressWarnings("rawtypes")
	@ParameterizedTest
	@RedisTestContextsSource
	void importDump(RedisTestContext redis) throws Exception {
		List<DataStructure> records = exportToList(redis);
		RedisServerCommands<String, String> sync = redis.sync();
		sync.flushall();
		execute("import-dump", redis, this::configureDumpFileImportCommand);
		Assertions.assertEquals(records.size(), sync.dbsize());
	}

	private void configureDumpFileImportCommand(CommandLine.ParseResult parseResult) {
		DumpFileImportCommand command = parseResult.subcommand().commandSpec().commandLine().getCommand();
		FileImportOptions options = command.getOptions();
		options.setFiles(options.getFiles().stream().map(this::replace).collect(Collectors.toList()));
	}

	private void configureExportCommand(CommandLine.ParseResult parseResult) {
		FileExportCommand command = parseResult.subcommand().commandSpec().commandLine().getCommand();
		command.setFile(replace(command.getFile()));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importJSONElastic(RedisTestContext redis) throws Exception {
		execute("import-json-elastic", redis);
		RedisKeyCommands<String, String> sync = redis.sync();
		Assertions.assertEquals(2, sync.keys("estest:*").size());
		Map<String, String> doc1 = ((RedisHashCommands<String, String>) sync).hgetall("estest:doc1");
		Assertions.assertEquals("ruan", doc1.get("_source.name"));
		Assertions.assertEquals("3", doc1.get("_source.articles[1]"));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importJSON(RedisTestContext redis) throws Exception {
		execute("import-json", redis);
		RedisKeyCommands<String, String> sync = redis.sync();
		List<String> keys = sync.keys("beer:*");
		Assertions.assertEquals(BEER_JSON_COUNT, keys.size());
		Map<String, String> beer1 = ((RedisHashCommands<String, String>) sync).hgetall("beer:1");
		Assertions.assertEquals("Hocus Pocus", beer1.get("name"));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importXML(RedisTestContext redis) throws Exception {
		execute("import-xml", redis);
		RedisKeyCommands<String, String> sync = redis.sync();
		List<String> keys = sync.keys("trade:*");
		Assertions.assertEquals(3, keys.size());
		Map<String, String> trade1 = ((RedisHashCommands<String, String>) sync).hgetall("trade:1");
		Assertions.assertEquals("XYZ0001", trade1.get("isin"));
	}

	@SuppressWarnings("rawtypes")
	@ParameterizedTest
	@RedisTestContextsSource
	void exportJSON(RedisTestContext redis) throws Exception {
		List<DataStructure> records = exportToList(redis);
		RedisServerCommands<String, String> sync = redis.sync();
		Assertions.assertEquals(sync.dbsize(), records.size());
	}

	@SuppressWarnings("rawtypes")
	@ParameterizedTest
	@RedisTestContextsSource
	void exportJSONGz(RedisTestContext redis) throws Exception {
		Path file = tempFile("beers.json.gz");
		execute("import-json", redis);
		execute("export-json-gz", redis, this::configureExportCommand);
		JsonItemReaderBuilder<Map> builder = new JsonItemReaderBuilder<>();
		builder.name("json-file-reader");
		FileSystemResource resource = new FileSystemResource(file);
		builder.resource(
				new InputStreamResource(new GZIPInputStream(resource.getInputStream()), resource.getDescription()));
		JacksonJsonObjectReader<Map> objectReader = new JacksonJsonObjectReader<>(Map.class);
		objectReader.setMapper(new ObjectMapper());
		builder.jsonObjectReader(objectReader);
		JsonItemReader<Map> reader = builder.build();
		List<Map> records = readAll(reader);
		RedisKeyCommands<String, String> sync = redis.sync();
		Assertions.assertEquals(sync.keys("beer:*").size(), records.size());
	}

	@SuppressWarnings("rawtypes")
	private List<DataStructure> exportToList(RedisTestContext redis) throws Exception {
		Path file = tempFile("redis.json");
		generate(redis);
		execute("export-json", redis, this::configureExportCommand);
		JsonItemReaderBuilder<DataStructure> builder = new JsonItemReaderBuilder<>();
		builder.name("json-data-structure-file-reader");
		builder.resource(new FileSystemResource(file));
		JacksonJsonObjectReader<DataStructure> objectReader = new JacksonJsonObjectReader<>(DataStructure.class);
		objectReader.setMapper(new ObjectMapper());
		builder.jsonObjectReader(objectReader);
		JsonItemReader<DataStructure> reader = builder.build();
		return readAll(reader);
	}

	@SuppressWarnings("rawtypes")
	@ParameterizedTest
	@RedisTestContextsSource
	void exportXml(RedisTestContext redis) throws Exception {
		generate(redis);
		Path file = tempFile("redis.xml");
		execute("export-xml", redis, this::configureExportCommand);
		XmlItemReaderBuilder<DataStructure> builder = new XmlItemReaderBuilder<>();
		builder.name("xml-file-reader");
		builder.resource(new FileSystemResource(file));
		XmlObjectReader<DataStructure> xmlObjectReader = new XmlObjectReader<>(DataStructure.class);
		xmlObjectReader.setMapper(new XmlMapper());
		builder.xmlObjectReader(xmlObjectReader);
		XmlItemReader<DataStructure> reader = builder.build();
		List<DataStructure> records = readAll(reader);
		RedisModulesCommands<String, String> sync = redis.sync();
		Assertions.assertEquals(sync.dbsize(), records.size());
		for (DataStructure<String> record : records) {
			String key = record.getKey();
			switch (record.getType()) {
			case HASH:
				Assertions.assertEquals(record.getValue(), sync.hgetall(key));
				break;
			case STRING:
				Assertions.assertEquals(record.getValue(), sync.get(key));
				break;
			default:
				break;
			}
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importJsonAPI(RedisTestContext redis) throws Exception {
		// riot-file import hset --keyspace beer --keys id
		FileImportCommand command = new FileImportCommand();
		command.getTransferOptions().setProgressStyle(ProgressStyle.NONE);
		command.getOptions().setFiles(Collections.singletonList(BEERS_JSON_URL));
		HsetCommand hset = new HsetCommand();
		hset.getKeyOptions().setKeyspace("beer");
		hset.getKeyOptions().setKeys(new String[] { "id" });
		command.setRedisCommands(Collections.singletonList(hset));
		RiotFile riotFile = new RiotFile();
		configure(riotFile, redis);
		command.setApp(riotFile);
		command.call();
		RedisKeyCommands<String, String> sync = redis.sync();
		List<String> keys = sync.keys("beer:*");
		Assertions.assertEquals(BEER_JSON_COUNT, keys.size());
		Map<String, String> beer1 = ((RedisHashCommands<String, String>) sync).hgetall("beer:1");
		Assertions.assertEquals("Hocus Pocus", beer1.get("name"));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importJSONGzip(RedisTestContext redis) throws Exception {
		execute("import-json-gz", redis);
		RedisKeyCommands<String, String> sync = redis.sync();
		List<String> keys = sync.keys("beer:*");
		Assertions.assertEquals(BEER_JSON_COUNT, keys.size());
	}

}
