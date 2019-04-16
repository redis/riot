package com.redislabs.riot.cli;

import java.io.IOException;
import java.util.Map;

import org.springframework.batch.item.json.JsonItemReader;

import picocli.CommandLine.Command;

@Command(name = "json", description = "Import a JSON file", sortOptions = false)
public class JsonImportSubCommand extends AbstractFileImportSubCommand {

	@SuppressWarnings("unchecked")
	@Override
	protected JsonItemReader<Map<String, Object>> countingReader() throws IOException {
		return (JsonItemReader<Map<String, Object>>) builder().buildJson();
	}

}
