package com.redislabs.riot.cli;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.cli.db.DatabaseExportOptions;
import com.redislabs.riot.cli.file.FileExportOptions;
import com.redislabs.riot.redis.reader.FieldExtractor;
import com.redislabs.riot.redis.reader.KeyFieldValueMapProcessor;
import com.redislabs.riot.redis.reader.KeyValue;
import com.redislabs.riot.redis.reader.KeyValueMapProcessor;
import com.redislabs.riot.redis.reader.KeyValueReader;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "export", description = "Export data from Redis")
public class MapExportCommand extends ExportCommand<KeyValue, Map<String, Object>> {

	@ArgGroup(exclusive = false, heading = "Field processor options%n")
	private MapProcessorOptions processor = new MapProcessorOptions();
	@Option(names = "--key-regex", description = "Regular expression for key-field extraction", paramLabel = "<regex>")
	private String keyRegex;
	@ArgGroup(exclusive = false, heading = "File options%n", order = 3)
	private FileExportOptions file = new FileExportOptions();
	@ArgGroup(exclusive = false, heading = "Database options%n")
	private DatabaseExportOptions db = new DatabaseExportOptions();

	@Override
	protected ItemReader<KeyValue> reader() throws Exception {
		return reader(KeyValueReader.builder().timeout(getReaderOptions().getTimeout()).build());
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected List<ItemProcessor> processors() {
		return Arrays.asList(keyValueMapProcessor(), processor.processor());
	}

	private KeyValueMapProcessor keyValueMapProcessor() {
		if (keyRegex != null) {
			return new KeyFieldValueMapProcessor(FieldExtractor.builder().regex(keyRegex).build());
		}
		return new KeyValueMapProcessor();
	}

	@Override
	protected ItemWriter<Map<String, Object>> writer() throws Exception {
		if (db.getUrl() != null) {
			return db.writer();
		}
		return file.writer();
	}

	@Override
	protected String taskName() {
		return "Exporting";
	}

}
