package com.redis.riot.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

class FileTests {

	@Test
	void importJSON() throws UnexpectedInputException, ParseException, NonTransientResourceException, Exception {
		FileImportCommand command = FileImportCommand.builder().build();
		Iterator<Map<String, Object>> iterator = command.read(FileIntegrationTests.BEERS_JSON_URL);
		Assertions.assertTrue(iterator.hasNext());
		Map<String, Object> beer1 = iterator.next();
		Assertions.assertEquals(13, beer1.size());
		int count = 1;
		while (iterator.hasNext()) {
			iterator.next();
			count++;
		}
		Assertions.assertEquals(FileIntegrationTests.BEER_JSON_COUNT, count);
	}

	@Test
	void importCSV() throws UnexpectedInputException, ParseException, NonTransientResourceException, Exception {
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
		Assertions.assertEquals(FileIntegrationTests.BEER_CSV_COUNT, count);
	}

	@Test
	void fileExpansion() throws IOException {
		Path temp = Files.createTempDirectory("fileExpansion");
		Files.createFile(temp.resolve("file1.csv"));
		Files.createFile(temp.resolve("file2.csv"));
		FileImportCommand command = FileImportCommand.builder().build();
		List<ItemReader<Map<String, Object>>> readers = command.readers(temp.resolve("*.csv").toString());
		Assertions.assertEquals(2, readers.size());
	}

}
