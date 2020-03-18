package com.redislabs.riot.cli;

import java.util.Arrays;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemProcessor;

import com.redislabs.riot.cli.db.DatabaseExportOptions;
import com.redislabs.riot.cli.file.FileExportOptions;
import com.redislabs.riot.redis.reader.KeyValue;
import com.redislabs.riot.redis.reader.KeyValueMapProcessor;
import com.redislabs.riot.redis.reader.KeyValueReader;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "export", description = "Export data from Redis", sortOptions = false)
public class MapExportCommand extends ExportCommand<KeyValue, Map<String, Object>> {

	@Mixin
	private KeyValueProcessorOptions keyValueProcessorOptions = new KeyValueProcessorOptions();
	@Mixin
	private MapProcessorOptions mapProcessorOptions = new MapProcessorOptions();
	@ArgGroup(exclusive = false, heading = "File options%n")
	private FileExportOptions file = new FileExportOptions();
	@ArgGroup(exclusive = false, heading = "Database options%n")
	private DatabaseExportOptions db = new DatabaseExportOptions();

	@Override
	protected ItemReader<KeyValue> reader() throws Exception {
		return reader(KeyValueReader.builder().timeout(getTimeout()).build());
	}

	@Override
	protected ItemProcessor<KeyValue, Map<String, Object>> processor() throws Exception {
		KeyValueMapProcessor keyValueProcessor = keyValueProcessorOptions.processor();
		ItemProcessor<Map<String, Object>, Map<String, Object>> mapProcessor = mapProcessorOptions.processor();
		if (mapProcessor == null) {
			return keyValueProcessor;
		}
		CompositeItemProcessor<KeyValue, Map<String, Object>> processor = new CompositeItemProcessor<>();
		processor.setDelegates(Arrays.asList(keyValueProcessor, mapProcessor));
		return processor;
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
