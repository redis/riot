package com.redislabs.riot.file;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.core.io.FileSystemResource;

import io.lettuce.core.GeoArgs;

public class TestCsv extends AbstractFileTest {


	@Test
	public void export() throws Exception {
		Path file = tempFile("beers.csv");
		executeFile("/json/import-hash.txt");
		executeFile("/csv/export-hash.txt");
		String[] header = Files.readAllLines(file).get(0).split("\\|");
		FlatFileItemReaderBuilder<Map<String, String>> builder = new FlatFileItemReaderBuilder<>();
		builder.name("flat-file-reader");
		builder.resource(new FileSystemResource(file));
		builder.strict(true);
		builder.saveState(false);
		builder.linesToSkip(1);
		builder.fieldSetMapper(new MapFieldSetMapper());
		builder.recordSeparatorPolicy(new DefaultRecordSeparatorPolicy());
		FlatFileItemReaderBuilder.DelimitedBuilder<Map<String, String>> delimitedBuilder = builder.delimited();
		delimitedBuilder.delimiter("|");
		delimitedBuilder.names(header);
		FlatFileItemReader<Map<String, String>> reader = builder.build();
		List<Map<String, String>> records = readAll(reader);
		Assertions.assertEquals(commands().keys("beer:*").size(), records.size());
	}

	@Test
	public void importHash() throws Exception {
		executeFile("/csv/import-hash.txt");
		List<String> keys = commands().keys("beer:*");
		Assertions.assertEquals(COUNT, keys.size());
	}

	@Test
	public void importMultiHash() throws Exception {
		executeFile("/csv/import-multi-hash.txt");
		List<String> keys = commands().keys("beer:*");
		Assertions.assertEquals(COUNT, keys.size());
	}

	@Test
	public void importGeo() throws Exception {
		executeFile("/csv/import-geo.txt");
		Set<String> results = commands().georadius("airportgeo", -122.4194, 37.7749, 20, GeoArgs.Unit.mi);
		Assertions.assertTrue(results.contains("3469"));
		Assertions.assertTrue(results.contains("10360"));
		Assertions.assertTrue(results.contains("8982"));
	}

	@Test
	public void importProcessorHashDateFormat() throws Exception {
		executeFile("/csv/import-hash-processor.txt");
		List<String> keys = commands().keys("event:*");
		Assertions.assertEquals(568, keys.size());
		Map<String, String> event = commands().hgetall("event:248206");
		Instant date = Instant.ofEpochMilli(Long.parseLong(event.get("EpochStart")));
		Assertions.assertTrue(date.isBefore(Instant.now()));
		long index = Long.parseLong(event.get("index"));
		Assertions.assertTrue(index > 0);
	}

}
