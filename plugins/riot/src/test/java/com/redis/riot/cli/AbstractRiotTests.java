package com.redis.riot.cli;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.util.FileCopyUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.Document;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.SearchResults;
import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.search.SuggetOptions;
import com.redis.lettucemod.search.TextField.PhoneticMatcher;
import com.redis.lettucemod.timeseries.MRangeOptions;
import com.redis.lettucemod.timeseries.RangeResult;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.lettucemod.timeseries.TimeRange;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.riot.cli.ReplicationOptions.ReplicationMode;
import com.redis.riot.cli.operation.HsetCommand;
import com.redis.riot.core.FakerItemReader;
import com.redis.riot.core.MapGenerator;
import com.redis.riot.core.MapWithMetadataGenerator;
import com.redis.riot.core.resource.XmlItemReader;
import com.redis.riot.core.resource.XmlItemReaderBuilder;
import com.redis.riot.core.resource.XmlObjectReader;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.reader.GeneratorItemReader;
import com.redis.spring.batch.reader.GeneratorReaderOptions;
import com.redis.spring.batch.reader.KeyComparatorOptions;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.RedisTestContextsSource;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.GeoArgs;
import io.lettuce.core.Range;
import io.lettuce.core.RedisURI;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.sync.RedisGeoCommands;
import io.lettuce.core.api.sync.RedisHashCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.api.sync.RedisSetCommands;
import io.lettuce.core.api.sync.RedisSortedSetCommands;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.cluster.SlotHash;
import picocli.CommandLine;
import picocli.CommandLine.ParseResult;

@SuppressWarnings("unchecked")
public abstract class AbstractRiotTests extends AbstractTestBase {

	private final Logger log = LoggerFactory.getLogger(getClass());

	public static final String BEERS_JSON_URL = "https://storage.googleapis.com/jrx/beers.json";
	public static final int BEER_CSV_COUNT = 2410;
	public static final int BEER_JSON_COUNT = 216;
	private static final Duration IDLE_TIMEOUT = Duration.ofSeconds(1);

	protected static String name(Map<String, String> beer) {
		return beer.get("name");
	}

	protected static String style(Map<String, String> beer) {
		return beer.get("style");
	}

	protected static double abv(Map<String, String> beer) {
		return Double.parseDouble(beer.get("abv"));
	}

	protected List<String> testImport(String filename, String pattern, int count) throws Exception {
		execute(filename);
		RedisKeyCommands<String, String> sync = connection.sync();
		List<String> keys = sync.keys(pattern);
		Assertions.assertEquals(count, keys.size());
		return keys;
	}

	private static Path tempDir;

	@BeforeAll
	public void setupFiles() throws IOException {
		tempDir = Files.createTempDirectory(FileTests.class.getName());
	}

	private AbstractRedisClient targetClient;
	private StatefulRedisModulesConnection<String, String> targetConnection;

	@BeforeAll
	void setupTarget() {
		RedisServer target = getTargetRedisServer();
		target.start();
		targetClient = client(target);
		targetConnection = RedisModulesUtils.connection(targetClient);
	}

	@AfterAll
	void teardownTarget() {
		targetConnection.close();
		targetClient.shutdown();
		targetClient.getResources().shutdown();
		getTargetRedisServer().close();
	}

	protected abstract RedisServer getTargetRedisServer();

	@BeforeEach
	void flushAllTarget() {
		targetConnection.sync().flushall();
	}

	@Override
	protected ParseResult parse(RiotCommandLine commandLine, String filename) throws Exception {
		ParseResult parseResult = super.parse(commandLine, filename);
		Object commandObject = parseResult.subcommand().commandSpec().commandLine().getCommand();
		if (commandObject instanceof ReplicateCommand) {
			ReplicateCommand command = (ReplicateCommand) commandObject;
			command.getTargetRedisOptions().setUri(RedisURI.create(getTargetRedisServer().getRedisURI()));
			command.getTargetRedisOptions().setPort(0);
			command.getTargetRedisOptions().setHost(Optional.empty());
			ReplicationMode mode = command.getReplicationOptions().getMode();
			if (mode == ReplicationMode.LIVE || mode == ReplicationMode.LIVEONLY) {
				command.getFlushingOptions().setIdleTimeout(IDLE_TIMEOUT);
				command.getReplicationOptions().setNotificationQueueCapacity(100000);
			}
		}
		return parseResult;
	}

	@Nested
	class FileTests {

		@Test
		void apiImportJSON() throws UnexpectedInputException, ParseException, NonTransientResourceException, Exception {
			FileImportCommand command = FileImportCommand.builder().build();
			Iterator<Map<String, Object>> iterator = command.read(AbstractRiotTests.BEERS_JSON_URL);
			Assertions.assertTrue(iterator.hasNext());
			Map<String, Object> beer1 = iterator.next();
			Assertions.assertEquals(13, beer1.size());
			int count = 1;
			while (iterator.hasNext()) {
				iterator.next();
				count++;
			}
			Assertions.assertEquals(AbstractRiotTests.BEER_JSON_COUNT, count);
		}

		@Test
		void apiImportCSV() throws UnexpectedInputException, ParseException, NonTransientResourceException, Exception {
			FileImportCommand command = FileImportCommand.builder()
					.flatFileOptions(FlatFileOptions.builder().header(true).build()).build();
			Iterator<Map<String, Object>> iterator = command.read("https://storage.googleapis.com/jrx/beers.csv");
			Assertions.assertTrue(iterator.hasNext());
			Map<String, Object> beer1 = iterator.next();
			Assertions.assertEquals(7, beer1.size());
			int count = 1;
			while (iterator.hasNext()) {
				iterator.next();
				count++;
			}
			Assertions.assertEquals(AbstractRiotTests.BEER_CSV_COUNT, count);
		}

		@Test
		void apiFileExpansion() throws IOException {
			Path temp = Files.createTempDirectory("fileExpansion");
			Files.createFile(temp.resolve("file1.csv"));
			Files.createFile(temp.resolve("file2.csv"));
			FileImportCommand command = FileImportCommand.builder().build();
			List<ItemReader<Map<String, Object>>> readers = command.readers(temp.resolve("*.csv").toString());
			Assertions.assertEquals(2, readers.size());
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

		@Test
		void importFW() throws Exception {
			testImport("file-import-fw", "account:*", 5);
			RedisHashCommands<String, String> hash = connection.sync();
			Map<String, String> account101 = hash.hgetall("account:101");
			// Account LastName FirstName Balance CreditLimit AccountCreated Rating
			// 101 Reeves Keanu 9315.45 10000.00 1/17/1998 A
			Assertions.assertEquals("Reeves", account101.get("LastName"));
			Assertions.assertEquals("Keanu", account101.get("FirstName"));
			Assertions.assertEquals("A", account101.get("Rating"));
		}

		@Test
		void importCSV() throws Exception {
			testImport("file-import-csv", "beer:*", BEER_CSV_COUNT);
		}

		@Test
		void importCSVSkipLines() throws Exception {
			testImport("file-import-csv-skiplines", "beer:*", BEER_CSV_COUNT - 10);
		}

		@Test
		void importCSVMax() throws Exception {
			testImport("file-import-csv-max", "beer:*", 12);
		}

		@Test
		void importPSV() throws Exception {
			testImport("file-import-psv", "sample:*", 3);
		}

		@Test
		void importTSV() throws Exception {
			testImport("file-import-tsv", "sample:*", 4);
		}

		@Test
		void importType() throws Exception {
			testImport("file-import-type", "sample:*", 3);
		}

		@Test
		void importExclude() throws Exception {
			execute("file-import-exclude");
			RedisHashCommands<String, String> sync = connection.sync();
			Map<String, String> beer1036 = sync.hgetall("beer:1036");
			Assertions.assertEquals("Lower De Boom", name(beer1036));
			Assertions.assertEquals("American Barleywine", style(beer1036));
			Assertions.assertEquals("368", beer1036.get("brewery_id"));
			Assertions.assertFalse(beer1036.containsKey("row"));
			Assertions.assertFalse(beer1036.containsKey("ibu"));
		}

		@Test
		void importInclude() throws Exception {
			execute("file-import-include");
			RedisHashCommands<String, String> sync = connection.sync();
			Map<String, String> beer1036 = sync.hgetall("beer:1036");
			Assertions.assertEquals(3, beer1036.size());
			Assertions.assertEquals("Lower De Boom", name(beer1036));
			Assertions.assertEquals("American Barleywine", style(beer1036));
			Assertions.assertEquals(0.099, abv(beer1036));
		}

		@Test
		void importFilter() throws Exception {
			testImport("file-import-filter", "beer:*", 424);
		}

		@Test
		void importRegex() throws Exception {
			execute("file-import-regex");
			RedisHashCommands<String, String> sync = connection.sync();
			Map<String, String> airport1 = sync.hgetall("airport:1");
			Assertions.assertEquals("Pacific", airport1.get("region"));
			Assertions.assertEquals("Port_Moresby", airport1.get("city"));
		}

		@Test
		void importGlob() throws Exception {
			execute("file-import-glob", this::configureImportGlob);
			RedisKeyCommands<String, String> sync = connection.sync();
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

		@Test
		void importGeoadd() throws Exception {
			execute("file-import-geoadd");
			RedisGeoCommands<String, String> sync = connection.sync();
			Set<String> results = sync.georadius("airportgeo", -21, 64, 200, GeoArgs.Unit.mi);
			Assertions.assertTrue(results.contains("18"));
			Assertions.assertTrue(results.contains("19"));
			Assertions.assertTrue(results.contains("11"));
		}

		@Test
		void importGeoProcessor() throws Exception {
			execute("file-import-geo-processor");
			RedisHashCommands<String, String> sync = connection.sync();
			Map<String, String> airport3469 = sync.hgetall("airport:18");
			Assertions.assertEquals("-21.9405994415,64.1299972534", airport3469.get("location"));
		}

		@Test
		void importProcess() throws Exception {
			testImport("file-import-process", "event:*", 568);
			RedisHashCommands<String, String> hash = connection.sync();
			Map<String, String> event = hash.hgetall("event:248206");
			Instant date = Instant.ofEpochMilli(Long.parseLong(event.get("EpochStart")));
			Assertions.assertTrue(date.isBefore(Instant.now()));
			long index = Long.parseLong(event.get("index"));
			Assertions.assertTrue(index > 0);
		}

		@Test
		void importProcessElvis() throws Exception {
			testImport("file-import-process-elvis", "beer:*", BEER_CSV_COUNT);
			Map<String, String> beer1436 = connection.sync().hgetall("beer:1436");
			Assertions.assertEquals("10", beer1436.get("ibu"));
		}

		@Test
		void importMultiCommands() throws Exception {
			execute("file-import-multi-commands");
			RedisKeyCommands<String, String> sync = connection.sync();
			List<String> beers = sync.keys("beer:*");
			Assertions.assertEquals(BEER_CSV_COUNT, beers.size());
			for (String beer : beers) {
				Map<String, String> hash = ((RedisHashCommands<String, String>) sync).hgetall(beer);
				Assertions.assertTrue(hash.containsKey("name"));
				Assertions.assertTrue(hash.containsKey("brewery_id"));
			}
			RedisSetCommands<String, String> set = connection.sync();
			Set<String> breweries = set.smembers("breweries");
			Assertions.assertEquals(558, breweries.size());
		}

		@Test
		void importBad() throws Exception {
			Assertions.assertEquals(0, execute("file-import-bad"));
		}

		@Test
		void importGCS() throws Exception {
			testImport("file-import-gcs", "beer:*", 4432);
			Map<String, String> beer1 = connection.sync().hgetall("beer:1");
			Assertions.assertEquals("Hocus Pocus", name(beer1));
		}

		@Test
		void importS3() throws Exception {
			testImport("file-import-s3", "beer:*", 4432);
			Map<String, String> beer1 = connection.sync().hgetall("beer:1");
			Assertions.assertEquals("Hocus Pocus", name(beer1));
		}

		@SuppressWarnings("rawtypes")
		@Test
		void importDump() throws Exception {
			List<DataStructure> records = exportToList();
			RedisServerCommands<String, String> sync = connection.sync();
			sync.flushall();
			execute("file-dump-import", this::configureDumpFileImportCommand);
			Assertions.assertEquals(records.size(), sync.dbsize());
		}

		private void configureDumpFileImportCommand(CommandLine.ParseResult parseResult) {
			FileDumpImportCommand command = parseResult.subcommand().commandSpec().commandLine().getCommand();
			FileImportOptions options = command.getOptions();
			options.setFiles(options.getFiles().stream().map(this::replace).collect(Collectors.toList()));
		}

		private void configureExportCommand(CommandLine.ParseResult parseResult) {
			FileExportCommand command = parseResult.subcommand().commandSpec().commandLine().getCommand();
			command.setFile(replace(command.getFile()));
		}

		@Test
		void importJSONElastic() throws Exception {
			execute("file-import-json-elastic");
			RedisKeyCommands<String, String> sync = connection.sync();
			Assertions.assertEquals(2, sync.keys("estest:*").size());
			Map<String, String> doc1 = ((RedisHashCommands<String, String>) sync).hgetall("estest:doc1");
			Assertions.assertEquals("ruan", doc1.get("_source.name"));
			Assertions.assertEquals("3", doc1.get("_source.articles[1]"));
		}

		@Test
		void importJSON() throws Exception {
			testImport("file-import-json", "beer:*", BEER_JSON_COUNT);
			Map<String, String> beer1 = connection.sync().hgetall("beer:1");
			Assertions.assertEquals("Hocus Pocus", beer1.get("name"));
		}

		@Test
		void importXML() throws Exception {
			testImport("file-import-xml", "trade:*", 3);
			Map<String, String> trade1 = connection.sync().hgetall("trade:1");
			Assertions.assertEquals("XYZ0001", trade1.get("isin"));
		}

		@SuppressWarnings("rawtypes")
		@Test
		void exportJSON() throws Exception {
			List<DataStructure> records = exportToList();
			RedisServerCommands<String, String> sync = connection.sync();
			Assertions.assertEquals(sync.dbsize(), records.size());
		}

		@SuppressWarnings("rawtypes")
		@Test
		void exportJSONGz() throws Exception {
			Path file = tempFile("beers.json.gz");
			execute("file-import-json");
			execute("file-export-json-gz", this::configureExportCommand);
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
			RedisKeyCommands<String, String> sync = connection.sync();
			Assertions.assertEquals(sync.keys("beer:*").size(), records.size());
		}

		@SuppressWarnings("rawtypes")
		private List<DataStructure> exportToList() throws Exception {
			Path file = tempFile("redis.json");
			generate();
			execute("file-export-json", this::configureExportCommand);
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
		@Test
		void exportXml() throws Exception {
			generate();
			Path file = tempFile("redis.xml");
			execute("file-export-xml", this::configureExportCommand);
			XmlItemReaderBuilder<DataStructure> builder = new XmlItemReaderBuilder<>();
			builder.name("xml-file-reader");
			builder.resource(new FileSystemResource(file));
			XmlObjectReader<DataStructure> xmlObjectReader = new XmlObjectReader<>(DataStructure.class);
			xmlObjectReader.setMapper(new XmlMapper());
			builder.xmlObjectReader(xmlObjectReader);
			XmlItemReader<DataStructure> reader = builder.build();
			List<DataStructure> records = readAll(reader);
			RedisModulesCommands<String, String> sync = connection.sync();
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

		@Test
		void importJsonAPI() throws Exception {
			// riot-file import hset --keyspace beer --keys id
			FileImportCommand command = new FileImportCommand();
			command.getTransferOptions().setProgressStyle(ProgressStyle.NONE);
			command.getOptions().setFiles(Collections.singletonList(BEERS_JSON_URL));
			HsetCommand hset = new HsetCommand();
			hset.getKeyOptions().setKeyspace("beer");
			hset.getKeyOptions().setKeys(new String[] { "id" });
			command.setRedisCommands(Collections.singletonList(hset));
			Main main = new Main();
			main.getRedisOptions().setUri(RedisURI.create(getRedisServer().getRedisURI()));
			main.getRedisOptions().setCluster(getRedisServer().isCluster());
			command.setApp(main);
			command.call();
			RedisKeyCommands<String, String> sync = connection.sync();
			List<String> keys = sync.keys("beer:*");
			Assertions.assertEquals(BEER_JSON_COUNT, keys.size());
			Map<String, String> beer1 = ((RedisHashCommands<String, String>) sync).hgetall("beer:1");
			Assertions.assertEquals("Hocus Pocus", beer1.get("name"));
		}

		@Test
		void importJSONGzip() throws Exception {
			testImport("file-import-json-gz", "beer:*", BEER_JSON_COUNT);
		}

		@Test
		void importSugadd() throws Exception {
			assertExecutionSuccessful(execute("file-import-sugadd"));
			List<Suggestion<String>> suggestions = connection.sync().ftSugget("names", "Bea",
					SuggetOptions.builder().withPayloads(true).build());
			Assertions.assertEquals(5, suggestions.size());
			Assertions.assertEquals("American Blonde Ale", suggestions.get(0).getPayload());
		}

		@Test
		void importElasticJSON() throws Exception {
			assertExecutionSuccessful(execute("file-import-json-elastic-jsonset"));
			RedisModulesCommands<String, String> sync = connection.sync();
			Assertions.assertEquals(2, sync.keys("elastic:*").size());
			ObjectMapper mapper = new ObjectMapper();
			String doc1 = sync.jsonGet("elastic:doc1");
			String expected = "{\"_index\":\"test-index\",\"_type\":\"docs\",\"_id\":\"doc1\",\"_score\":1,\"_source\":{\"name\":\"ruan\",\"age\":30,\"articles\":[\"1\",\"3\"]}}";
			Assertions.assertEquals(mapper.readTree(expected), mapper.readTree(doc1));
		}
	}

	@Nested
	class GenTests {

		@Test
		void testReader() throws Exception {
			int count = 100;
			Map<String, String> fields = new HashMap<>();
			fields.put("firstName", "name.firstName");
			fields.put("lastName", "name.lastName");
			FakerItemReader reader = new FakerItemReader(MapGenerator.builder().fields(fields).build());
			reader.setCount(count);
			List<Map<String, Object>> items = new ArrayList<>();
			String name = UUID.randomUUID().toString();
			run(name, DEFAULT_BATCH_SIZE, reader, items::addAll);
			Assertions.assertEquals(count, items.size());
			Assertions.assertFalse(items.get(0).containsKey(MapGenerator.FIELD_INDEX));
			Assertions.assertTrue(((String) items.get(0).get("firstName")).length() > 0);
			Assertions.assertTrue(((String) items.get(0).get("lastName")).length() > 0);
		}

		@Test
		void includeMetadata() throws Exception {
			int count = 100;
			Map<String, String> fields = new HashMap<>();
			fields.put("firstName", "name.firstName");
			fields.put("lastName", "name.lastName");
			FakerItemReader reader = new FakerItemReader(
					new MapWithMetadataGenerator(MapGenerator.builder().fields(fields).build()));
			reader.setCount(count);
			List<Map<String, Object>> items = new ArrayList<>();
			String name = UUID.randomUUID().toString();
			run(name, DEFAULT_BATCH_SIZE, reader, items::addAll);
			Assertions.assertEquals(count, items.size());
			Assertions.assertEquals(1, items.get(0).get(MapGenerator.FIELD_INDEX));
		}

		@Test
		@RedisTestContextsSource
		void fakerHash() throws Exception {
			List<String> keys = testImport("faker-hset", "person:*", 1000);
			Map<String, String> person = connection.sync().hgetall(keys.get(0));
			Assertions.assertTrue(person.containsKey("firstName"));
			Assertions.assertTrue(person.containsKey("lastName"));
			Assertions.assertTrue(person.containsKey("address"));
		}

		@Test
		@RedisTestContextsSource
		void fakerSet() throws Exception {
			execute("faker-sadd");
			RedisSetCommands<String, String> sync = connection.sync();
			Set<String> names = sync.smembers("got:characters");
			Assertions.assertTrue(names.size() > 10);
			for (String name : names) {
				Assertions.assertFalse(name.isEmpty());
			}
		}

		@Test
		void fakerZset() throws Exception {
			execute("faker-zadd");
			RedisKeyCommands<String, String> sync = connection.sync();
			List<String> keys = sync.keys("leases:*");
			Assertions.assertTrue(keys.size() > 100);
			String key = keys.get(0);
			Assertions.assertTrue(((RedisSortedSetCommands<String, String>) sync).zcard(key) > 0);
		}

		@Test
		void fakerStream() throws Exception {
			execute("faker-xadd");
			RedisStreamCommands<String, String> sync = connection.sync();
			List<StreamMessage<String, String>> messages = sync.xrange("teststream:1", Range.unbounded());
			Assertions.assertTrue(messages.size() > 0);
		}

		@Test
		void fakerInfer() throws Exception {
			String INDEX = "beerIdx";
			String FIELD_ID = "id";
			String FIELD_ABV = "abv";
			String FIELD_NAME = "name";
			String FIELD_STYLE = "style";
			String FIELD_OUNCES = "ounces";
			connection.sync().ftCreate(INDEX, CreateOptions.<String, String>builder().prefix("beer:").build(),
					Field.tag(FIELD_ID).sortable().build(), Field.text(FIELD_NAME).sortable().build(),
					Field.text(FIELD_STYLE).matcher(PhoneticMatcher.ENGLISH).sortable().build(),
					Field.numeric(FIELD_ABV).sortable().build(), Field.numeric(FIELD_OUNCES).sortable().build());
			execute("faker-infer");
			SearchResults<String, String> results = connection.sync().ftSearch(INDEX, "*");
			Assertions.assertEquals(1000, results.getCount());
			Document<String, String> doc1 = results.get(0);
			Assertions.assertNotNull(doc1.get(FIELD_ABV));
		}

		@Test
		void fakerTsAdd() throws Exception {
			execute("faker-tsadd");
			List<Sample> samples = connection.sync().tsRange("ts:gen", TimeRange.unbounded(), null);
			Assertions.assertEquals(10, samples.size());
		}

		@Test
		void fakerTsAddWithOptions() throws Exception {
			execute("faker-tsadd-options");
			List<RangeResult<String, String>> results = connection.sync().tsMrange(TimeRange.unbounded(),
					MRangeOptions.<String, String>filters("character1=Einstein").build());
			Assertions.assertFalse(results.isEmpty());
		}

		@Test
		void generateTypes() throws Exception {
			execute("generate");
			Assertions.assertEquals(100, connection.sync().dbsize());
		}

	}

	@Nested
	class Replication {

		@Test
		void keyDumps() throws Throwable {
			generate();
			Assertions.assertTrue(connection.sync().dbsize() > 0);
			execute("replicate");
		}

		@Test
		void dryRun() throws Throwable {
			generate();
			Assertions.assertTrue(connection.sync().dbsize() > 0);
			execute("replicate-dry-run");
			Assertions.assertEquals(0, targetConnection.sync().dbsize());
		}

		@Test
		void hyperLogLog() throws Throwable {
			String key = "crawled:20171124";
			String value = "http://www.google.com/";
			connection.sync().pfadd(key, value);
			Assertions.assertEquals(0, execute("replicate-hll"));
			Assertions.assertTrue(compare());
		}

		private void awaitCompare() {
			Awaitility.await().timeout(Duration.ofSeconds(1)).until(this::compare);
		}

		@Test
		void keyProcessor() throws Throwable {
			generate(200);
			Long sourceSize = connection.sync().dbsize();
			Assertions.assertTrue(sourceSize > 0);
			execute("replicate-key-processor");
			Assertions.assertEquals(sourceSize, targetConnection.sync().dbsize());
			Assertions.assertEquals(connection.sync().get("string:123"), targetConnection.sync().get("0:string:123"));
		}

		@Test
		void live() throws Exception {
			runLiveReplication("replicate-live");
		}

		@Test
		void liveKeySlot() throws Exception {
			connection.sync().configSet("notify-keyspace-events", "AK");
			ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
			executor.schedule(() -> {
				try {
					generate(1, GeneratorReaderOptions.builder().build(), 300);
				} catch (Exception e) {
					log.error("Could not generate data", e);
				}
			}, 500, TimeUnit.MILLISECONDS);
			execute("replicate-live-keyslot");
			List<String> keys = targetConnection.sync().keys("*");
			for (String key : keys) {
				int slot = SlotHash.getSlot(key);
				Assertions.assertTrue(slot >= 0 && slot <= 8000);
			}
		}

		@Test
		void liveMultiThreaded() throws Exception {
			runLiveReplication("replicate-live-threads");
		}

		@Test
		void liveDataStructures() throws Exception {
			runLiveReplication("replicate-ds-live");
		}

		private void runLiveReplication(String filename) throws Exception {
			connection.sync().configSet("notify-keyspace-events", "AK");
			generate(3000);
			ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
			executor.schedule(() -> {
				GeneratorItemReader updateReader = new GeneratorItemReader(GeneratorReaderOptions.builder().build());
				updateReader.setCurrentItemCount(3000);
				updateReader.setMaxItemCount(5000);
				try {
					run("compare-live-replication-" + id(), 1, updateReader, writer(client).dataStructure());
				} catch (Exception e) {
					log.error("Could not generate data", e);
				}
			}, 500, TimeUnit.MILLISECONDS);
			execute(filename);
			awaitCompare();
		}

		protected RedisItemReader<String, KeyComparison> comparisonReader() {
			return reader(client).comparator(targetClient)
					.comparatorOptions(KeyComparatorOptions.builder().ttlTolerance(Duration.ofMillis(100)).build())
					.build();
		}

		protected boolean compare() throws JobExecutionException {
			if (connection.sync().dbsize().equals(0L)) {
				return false;
			}
			if (connection.sync().dbsize() != targetConnection.sync().dbsize()) {
				return false;
			}
			RedisItemReader<String, KeyComparison> reader = comparisonReader();
			SynchronizedListItemWriter<KeyComparison> writer = new SynchronizedListItemWriter<>();
			run("compare-" + id(), DEFAULT_BATCH_SIZE, reader, writer);
			awaitClosed(reader);
			if (writer.getWrittenItems().isEmpty()) {
				return false;
			}
			for (KeyComparison comparison : writer.getWrittenItems()) {
				if (comparison.getStatus() != Status.OK) {
					return false;
				}
			}
			return true;
		}

	}

}
