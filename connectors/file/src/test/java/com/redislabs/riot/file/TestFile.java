package com.redislabs.riot.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.test.DataGenerator;
import io.lettuce.core.GeoArgs;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import com.redislabs.riot.RiotApp;
import com.redislabs.riot.test.AbstractStandaloneRedisTest;
import org.springframework.batch.item.xml.XmlItemReader;
import org.springframework.batch.item.xml.XmlObjectReader;
import org.springframework.batch.item.xml.support.XmlItemReaderBuilder;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;

public class TestFile extends AbstractStandaloneRedisTest {

	protected final static int COUNT = 2410;

	private static Path tempDir;

	@BeforeAll
	public static void setupAll() throws IOException {
		tempDir = Files.createTempDirectory(TestFile.class.getName());
	}

	protected Path tempFile(String filename) throws IOException {
		Path path = tempDir.resolve(filename);
		if (Files.exists(path)) {
			Files.delete(path);
		}
		return path;
	}

	protected static String name(Map<String,String> beer) {
		return beer.get("name");
	}

	protected static String style(Map<String,String> beer) {
		return beer.get("style");
	}

	protected static double abv(Map<String,String> beer) {
		return Double.parseDouble(beer.get("abv"));
	}

	@Override
	protected String process(String command) {
		return super.process(command).replace("/tmp", tempDir.toString());
	}

	@Override
	protected RiotApp app() {
		return new RiotFile();
	}

	@Override
	protected String appName() {
		return "riot-file";
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
	public void importCSV() throws Exception {
		executeFile("import-csv");
		List<String> keys = sync.keys("beer:*");
		Assertions.assertEquals(COUNT, keys.size());
	}

	@Test
	public void importPSV() throws Exception {
		executeFile("import-psv");
		List<String> keys = sync.keys("sample:*");
		Assertions.assertEquals(3, keys.size());
	}

	@Test
	public void importTSV() throws Exception {
		executeFile("import-tsv");
		List<String> keys = sync.keys("sample:*");
		Assertions.assertEquals(4, keys.size());
	}

	@Test
	public void importType() throws Exception {
		executeFile("import-type");
		List<String> keys = sync.keys("sample:*");
		Assertions.assertEquals(3, keys.size());
	}

	@Test
	public void importExclude() throws Exception {
		executeFile("import-exclude");
		Map<String, String> beer1036 = sync.hgetall("beer:1036");
		Assertions.assertEquals("Lower De Boom", name(beer1036));
		Assertions.assertEquals("American Barleywine", style(beer1036));
		Assertions.assertEquals("368", beer1036.get("brewery_id"));
		Assertions.assertFalse(beer1036.containsKey("row"));
		Assertions.assertFalse(beer1036.containsKey("ibu"));
	}

	@Test
	public void importInclude() throws Exception {
		executeFile("import-include");
		Map<String, String> beer1036 = sync.hgetall("beer:1036");
		Assertions.assertEquals(3, beer1036.size());
		Assertions.assertEquals("Lower De Boom", name(beer1036));
		Assertions.assertEquals("American Barleywine", style(beer1036));
		Assertions.assertEquals(0.099, abv(beer1036));
	}

	@Test
	public void importFilter() throws Exception {
		executeFile("import-filter");
		List<String> keys = sync.keys("beer:*");
		Assertions.assertEquals(424, keys.size());
	}

	@Test
	public void importRegex() throws Exception {
		executeFile("import-regex");
		Map<String, String> airport1 = sync.hgetall("airport:1");
		Assertions.assertEquals("Pacific", airport1.get("region"));
		Assertions.assertEquals("Port_Moresby", airport1.get("city"));
	}

	@Test
	public void importGlob() throws Exception {
		executeFile("import-glob");
		List<String> keys = sync.keys("beer:*");
		Assertions.assertEquals(COUNT, keys.size());
	}

	@Test
	public void importGeoadd() throws Exception {
		executeFile("import-geoadd");
		Set<String> results = sync.georadius("airportgeo", -122.4194, 37.7749, 20, GeoArgs.Unit.mi);
		Assertions.assertTrue(results.contains("3469"));
		Assertions.assertTrue(results.contains("10360"));
		Assertions.assertTrue(results.contains("8982"));
	}

	@Test
	public void importProcess() throws Exception {
		executeFile("import-process");
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
		executeFile("import-multi-commands");
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

	@Test
	public void importBad() throws Exception {
		executeFile("import-bad");
	}

	@Test
	public void importGCS() throws Exception {
		executeFile("import-gcs");
		List<String> keys = sync.keys("beer:*");
		Assertions.assertEquals(4432, keys.size());
		Map<String, String> beer1 = sync.hgetall("beer:1");
		Assertions.assertEquals("Hocus Pocus", name(beer1));
	}

	@Test
	public void importS3() throws Exception {
		executeFile("import-s3");
		List<String> keys = sync.keys("beer:*");
		Assertions.assertEquals(4432, keys.size());
		Map<String, String> beer1 = sync.hgetall("beer:1");
		Assertions.assertEquals("Hocus Pocus", name(beer1));
	}


	@Test
	public void exportJSON() throws Exception {
		List<DataStructure> records = exportToList();
		Assertions.assertEquals(sync.dbsize(), records.size());
	}

	private List<DataStructure> exportToList() throws Exception {
		Path file = tempFile("redis.json");
		DataGenerator.builder().commands(async).build().run();
		executeFile("export-json");
		JsonItemReaderBuilder<DataStructure> builder = new JsonItemReaderBuilder<>();
		builder.name("json-data-structure-file-reader");
		builder.resource(new FileSystemResource(file));
		JacksonJsonObjectReader<DataStructure> objectReader = new JacksonJsonObjectReader<>(DataStructure.class);
		objectReader.setMapper(new ObjectMapper());
		builder.jsonObjectReader(objectReader);
		JsonItemReader<DataStructure> reader = builder.build();
		return readAll(reader);
	}

	@Test
	public void importDump() throws Exception {
		List<DataStructure> records = exportToList();
		sync.flushall();
		executeFile("import-dump");
		Assertions.assertEquals(records.size(), sync.dbsize());
	}

	@Test
	public void exportJsonGz() throws Exception {
		Path file = tempFile("beers.json.gz");
		executeFile("import-json");
		executeFile("export-json-gz");
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
		Assertions.assertEquals(sync.keys("beer:*").size(), records.size());
	}

	@Test
	public void importJsonElastic() throws Exception {
		executeFile("import-json-elastic");
		Assertions.assertEquals(2, sync.keys("estest:*").size());
		Map<String, String> doc1 = sync.hgetall("estest:doc1");
		Assertions.assertEquals("ruan", doc1.get("_source.name"));
		Assertions.assertEquals("3", doc1.get("_source.articles[1]"));
	}

	@Test
	public void importJson() throws Exception {
		executeFile("import-json");
		List<String> keys = sync.keys("beer:*");
		Assertions.assertEquals(4432, keys.size());
		Map<String, String> beer1 = sync.hgetall("beer:1");
		Assertions.assertEquals("Hocus Pocus", beer1.get("name"));
	}


	@Test
	public void importXml() throws Exception {
		executeFile("import-xml");
		List<String> keys = sync.keys("trade:*");
		Assertions.assertEquals(3, keys.size());
		Map<String, String> trade1 = sync.hgetall("trade:1");
		Assertions.assertEquals("XYZ0001", trade1.get("isin"));
	}

	@SuppressWarnings({ "incomplete-switch", "rawtypes", "unchecked" })
	@Test
	public void exportXml() throws Exception {
		DataGenerator.builder().commands(async).build().run();
		Path file = tempFile("redis.xml");
		executeFile("export-xml");
		XmlItemReaderBuilder<DataStructure> builder = new XmlItemReaderBuilder<>();
		builder.name("xml-file-reader");
		builder.resource(new FileSystemResource(file));
		XmlObjectReader<DataStructure> xmlObjectReader = new XmlObjectReader<>(DataStructure.class);
		xmlObjectReader.setMapper(new XmlMapper());
		builder.xmlObjectReader(xmlObjectReader);
		XmlItemReader<DataStructure<String>> reader = (XmlItemReader) builder.build();
		List<DataStructure<String>> records = readAll(reader);
		Assertions.assertEquals(sync.dbsize(), records.size());
		RedisCommands<String, String> commands = sync;
		for (DataStructure<String> record : records) {
			String key = record.getKey();
			switch (record.getType()) {
				case HASH:
					Assertions.assertEquals(record.getValue(), commands.hgetall(key));
					break;
				case STRING:
					Assertions.assertEquals(record.getValue(), commands.get(key));
					break;
			}
		}
	}

}
