package com.redislabs.riot;

import java.io.File;
import java.nio.file.Files;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.DelimitedBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.lettusearch.search.Schema;
import com.redislabs.lettusearch.search.SearchResult;
import com.redislabs.lettusearch.search.SearchResults;
import com.redislabs.lettusearch.search.field.Field;
import com.redislabs.lettusearch.search.field.PhoneticMatcher;
import com.redislabs.riot.cli.file.MapFieldSetMapper;

import io.lettuce.core.GeoArgs.Unit;

public class TestFile extends BaseTest {

	@Test
	public void testExportCsv() throws UnexpectedInputException, ParseException, Exception {
		File file = new File("/tmp/beers.csv");
		file.delete();
		runFile("file-import-json-hash");
		runFile("file-export-csv");
		String[] header = Files.readAllLines(file.toPath()).get(0).split("\\|");
		FlatFileItemReaderBuilder<Map<String, Object>> builder = new FlatFileItemReaderBuilder<Map<String, Object>>();
		builder.name("flat-file-reader");
		builder.resource(new FileSystemResource(file));
		builder.strict(true);
		builder.saveState(false);
		builder.linesToSkip(1);
		builder.fieldSetMapper(new MapFieldSetMapper());
		builder.recordSeparatorPolicy(new DefaultRecordSeparatorPolicy());
		DelimitedBuilder<Map<String, Object>> delimitedBuilder = builder.delimited();
		delimitedBuilder.delimiter("|");
		delimitedBuilder.names(header);
		FlatFileItemReader<Map<String, Object>> reader = builder.build();
		List<Map<String, Object>> records = readAll(reader);
		Assertions.assertEquals(commands().keys("beer:*").size(), records.size());
	}

	private <T> List<T> readAll(AbstractItemCountingItemStreamItemReader<T> reader)
			throws UnexpectedInputException, ParseException, Exception {
		reader.open(new ExecutionContext());
		List<T> records = new ArrayList<>();
		T record;
		while ((record = reader.read()) != null) {
			records.add(record);
		}
		reader.close();
		return records;
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testExportJson() throws UnexpectedInputException, ParseException, Exception {
		File file = new File("/tmp/beers.json");
		file.delete();
		runFile("file-import-json-hash");
		runFile("file-export-json");
		JsonItemReaderBuilder<Map> builder = new JsonItemReaderBuilder<>();
		builder.name("json-file-reader");
		builder.resource(new FileSystemResource(file));
		JacksonJsonObjectReader<Map> objectReader = new JacksonJsonObjectReader<>(Map.class);
		objectReader.setMapper(new ObjectMapper());
		builder.jsonObjectReader(objectReader);
		JsonItemReader<Map> reader = builder.build();
		List<Map> records = readAll(reader);
		Assertions.assertEquals(commands().keys("beer:*").size(), records.size());
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testExportJsonGz() throws UnexpectedInputException, ParseException, Exception {
		File file = new File("/tmp/beers.json.gz");
		file.delete();
		runFile("file-import-json-hash");
		runFile("file-export-json_gz");
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
		Assertions.assertEquals(commands().keys("beer:*").size(), records.size());
	}

	@Test
	public void testImportCsvHash() throws Exception {
		runFile("file-import-csv-hash");
		List<String> keys = commands().keys("beer:*");
		Assertions.assertEquals(BEER_COUNT, keys.size());
	}

	@Test
	public void testImportCsvSearch() throws Exception {
		String FIELD_ABV = "abv";
		String FIELD_NAME = "name";
		String FIELD_STYLE = "style";
		String FIELD_OUNCES = "ounces";
		String INDEX = "beers";
		commands().flushall();
		Schema schema = Schema.builder().field(Field.text(FIELD_NAME).sortable(true))
				.field(Field.text(FIELD_STYLE).matcher(PhoneticMatcher.English).sortable(true))
				.field(Field.numeric(FIELD_ABV).sortable(true)).field(Field.numeric(FIELD_OUNCES).sortable(true))
				.build();
		commands().create(INDEX, schema);
		runFile("file-import-csv-search");
		SearchResults<String, String> results = commands().search(INDEX, "*");
		Assertions.assertEquals(BEER_COUNT, results.count());
	}

	@Test
	public void testImportCsvProcessorSearchGeo() throws Exception {
		String INDEX = "airports";
		commands().flushall();
		Schema schema = Schema.builder().field(Field.text("Name").sortable(true))
				.field(Field.geo("Location").sortable(true)).build();
		commands().create(INDEX, schema);
		runFile("file-import-csv-processor-search-geo");
		SearchResults<String, String> results = commands().search(INDEX, "@Location:[-77 38 50 mi]");
		Assertions.assertEquals(3, results.count());
	}

	@Test
	public void testImportCsvGeo() throws Exception {
		runFile("file-import-csv-geo");
		Set<String> results = commands().georadius("airportgeo", -122.4194, 37.7749, 20, Unit.mi);
		Assertions.assertTrue(results.contains("3469"));
		Assertions.assertTrue(results.contains("10360"));
		Assertions.assertTrue(results.contains("8982"));
	}

	@Test
	public void testImportElasticJson() throws Exception {
		String url = getClass().getClassLoader().getResource("es_test-index.json").getFile();
		runFile("file-import-elastic-json", url);
		Assertions.assertEquals(2, commands().keys("estest:*").size());
		Map<String, String> doc1 = commands().hgetall("estest:doc1");
		Assertions.assertEquals("ruan", doc1.get("_source.name"));
		Assertions.assertEquals("3", doc1.get("_source.articles[1]"));
	}

	@Test
	public void testImportJsonHash() throws Exception {
		runFile("file-import-json-hash");
		List<String> keys = commands().keys("beer:*");
		Assertions.assertEquals(4432, keys.size());
		Map<String, String> beer1 = commands().hgetall("beer:1");
		Assertions.assertEquals("Hocus Pocus", beer1.get("name"));
	}

	@Test
	public void testImportCsvProcessorHashDateFormat() throws Exception {
		runFile("file-import-csv-processor-hash-dateformat");
		List<String> keys = commands().keys("event:*");
		Assertions.assertEquals(568, keys.size());
		Map<String, String> event = commands().hgetall("event:248206");
		Instant date = Instant.ofEpochMilli(Long.parseLong(event.get("EpochStart")));
		Assertions.assertTrue(date.isBefore(Instant.now()));
		long index = Long.parseLong(event.get("index"));
		Assertions.assertTrue(index > 0);
	}

	@Test
	public void testImportCsvProcessorSearch() throws Exception {
		String INDEX = "laevents";
		commands().flushall();
		Schema schema = Schema.builder().field(Field.text("Title")).field(Field.numeric("lon"))
				.field(Field.numeric("kat")).field(Field.geo("location").sortable(true)).build();
		commands().create(INDEX, schema);
		runFile("file-import-csv-processor-search");
		SearchResults<String, String> results = commands().search(INDEX, "@location:[-118.446014 33.998415 10 mi]");
		Assertions.assertTrue(results.count() > 0);
		for (SearchResult<String, String> result : results) {
			Double lat = Double.parseDouble(result.get("lat"));
			Assertions.assertTrue(lat > 33 && lat < 35);
		}
	}
}
