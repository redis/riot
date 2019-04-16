package com.redislabs.riot.cli;

import java.io.IOException;
import java.util.Map;

import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.core.io.Resource;

import com.redislabs.riot.file.FileConfig;

import picocli.CommandLine.Command;

@Command(name = "json", description = "Import a JSON file", sortOptions = false)
public class JsonImportSubCommand extends AbstractFileImportSubCommand {

	@SuppressWarnings("unchecked")
	@Override
	protected JsonItemReader<Map<String, Object>> reader(Resource resource) throws IOException {
		return (JsonItemReader<Map<String, Object>>) new FileConfig().reader(resource);
	}

}
