package com.redislabs.riot.cli;

import java.io.IOException;
import java.util.Map;

import org.springframework.batch.item.json.JsonItemReader;

import picocli.CommandLine.Command;

@Command(name = "json", description = "Import a JSON file")
public class JsonImportSubCommand extends AbstractFileImportSubCommand {

	@SuppressWarnings("unchecked")
	@Override
	public JsonItemReader<Map<String, Object>> reader() throws IOException {
		return (JsonItemReader<Map<String, Object>>) builder().buildJson();
	}

}
