package com.redis.riot.operation;

import java.util.Map;

import org.springframework.batch.item.ItemStreamException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.redis.spring.batch.item.redis.writer.impl.JsonSet;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "json.set", description = "Add JSON documents to RedisJSON")
public class JsonSetCommand extends AbstractOperationCommand {

	private final ObjectWriter jsonWriter = new ObjectMapper().writerFor(Map.class);

	@Option(names = "--path", description = "Path field.", paramLabel = "<field>")
	private String pathField;

	@Override
	public JsonSet<String, String, Map<String, Object>> operation() {
		JsonSet<String, String, Map<String, Object>> operation = new JsonSet<>(keyFunction(), this::jsonValue);
		if (pathField != null) {
			operation.setPathFunction(toString(pathField));
		}
		return operation;
	}

	private String jsonValue(Map<String, Object> map) {
		try {
			return jsonWriter.writeValueAsString(map);
		} catch (JsonProcessingException e) {
			throw new ItemStreamException("Could not serialize to JSON", e);
		}
	}

}