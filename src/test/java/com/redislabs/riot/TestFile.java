package com.redislabs.riot;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.lettusearch.search.Schema;
import com.redislabs.lettusearch.search.Schema.SchemaBuilder;
import com.redislabs.lettusearch.search.SearchResults;
import com.redislabs.lettusearch.search.field.NumericField;
import com.redislabs.lettusearch.search.field.PhoneticMatcher;
import com.redislabs.lettusearch.search.field.TextField;
import com.redislabs.riot.cli.file.MapFieldSetMapper;

import io.lettuce.core.GeoArgs.Unit;

public class TestFile extends BaseTest {

	@Test
	public void testExportBeersCsv() throws UnexpectedInputException, ParseException, Exception {
		runFile("import-beers_json");
		runFile("export-beers_csv");
		File file = new File("/tmp/beers.csv");
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
	public void testExportBeersJson() throws UnexpectedInputException, ParseException, Exception {
		runFile("import-beers_json");
		runFile("export-beers_json");
		File file = new File("/tmp/beers.json");
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

	@Test
	public void testImportCsv() throws Exception {
		runFile("import-beers_csv");
		List<String> keys = commands().keys("beer:*");
		Assertions.assertEquals(2410, keys.size());
	}

	@Test
	public void testImportBeersSearch() throws Exception {
		String FIELD_ABV = "abv";
		String FIELD_NAME = "name";
		String FIELD_STYLE = "style";
		String FIELD_OUNCES = "ounces";
		String INDEX = "beers";
		commands().flushall();
		SchemaBuilder schema = Schema.builder();
		schema.field(TextField.builder().name(FIELD_NAME).sortable(true).build());
		schema.field(TextField.builder().name(FIELD_STYLE).matcher(PhoneticMatcher.English).sortable(true).build());
		schema.field(NumericField.builder().name(FIELD_ABV).sortable(true).build());
		schema.field(NumericField.builder().name(FIELD_OUNCES).sortable(true).build());
		commands().create(INDEX, schema.build());
		runFile("import-beers_csv-search");
		SearchResults<String, String> results = commands().search(INDEX, "*");
		Assertions.assertEquals(2410, results.getCount());
	}

	@Test
	public void testImportAirports() throws Exception {
		runFile("import-airports");
		Set<String> results = commands().georadius("airportgeo", -122.4194, 37.7749, 20, Unit.mi);
		Assertions.assertTrue(results.contains("3469"));
		Assertions.assertTrue(results.contains("10360"));
		Assertions.assertTrue(results.contains("8982"));
	}

	@Test
	public void testImportElasticacheJson() throws Exception {
		String url = getClass().getClassLoader().getResource("es_test-index.json").getFile();
		runCommand("import --keyspace estest --keys _id file %s", url);
		Assertions.assertEquals(2, commands().keys("estest:*").size());
		Map<String, String> doc1 = commands().hgetall("estest:doc1");
		Assertions.assertEquals("ruan", doc1.get("_source.name"));
		Assertions.assertEquals("1,3", doc1.get("_source.articles"));
	}

	@Test
	public void testImportBeersJson() throws Exception {
		runFile("import-beers_json");
		System.out.println("Before commands");
		List<String> keys = commands().keys("beer:*");
		Assertions.assertEquals(4432, keys.size());
		Map<String, String> beer1 = commands().hgetall("beer:1");
		Assertions.assertEquals("Hocus Pocus", beer1.get("name"));
	}
}
