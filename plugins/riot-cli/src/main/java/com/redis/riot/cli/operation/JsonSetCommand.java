package com.redis.riot.cli.operation;

import java.util.Map;

import org.springframework.batch.item.ItemStreamException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.redis.spring.batch.writer.operation.JsonSet;

import picocli.CommandLine.Command;

@Command(name = "json.set", description = "Add JSON documents to RedisJSON")
public class JsonSetCommand extends AbstractKeyCommand {

	private ObjectMapper mapper = new ObjectMapper();
	private ObjectWriter jsonWriter = mapper.writerFor(Map.class);

	public JsonSet<String, String, Map<String, Object>> operation() {
		return new JsonSet<>(key(), this::value);
	}

	private String value(Map<String, Object> map) {
		try {
			return jsonWriter.writeValueAsString(map);
		} catch (JsonProcessingException e) {
			throw new ItemStreamException("Could not serialize to JSON", e);
		}
	}

}
