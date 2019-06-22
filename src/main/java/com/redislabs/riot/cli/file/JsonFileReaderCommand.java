package com.redislabs.riot.cli.file;

import java.util.Map;

import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import com.fasterxml.jackson.databind.ObjectMapper;

import picocli.CommandLine.Command;

@Command(name = "json", description = "JSON file")
public class JsonFileReaderCommand extends AbstractFileReaderCommand {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader() throws Exception {
		JsonItemReaderBuilder<Map> builder = new JsonItemReaderBuilder<Map>();
		builder.name("json-file-reader");
		builder.resource(resource());
		JacksonJsonObjectReader<Map> objectReader = new JacksonJsonObjectReader<>(Map.class);
		objectReader.setMapper(new ObjectMapper());
		builder.jsonObjectReader(objectReader);
		JsonItemReader<? extends Map> reader = builder.build();
		return (AbstractItemCountingItemStreamItemReader<Map<String, Object>>) reader;
	}

}
