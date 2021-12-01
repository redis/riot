package com.redis.riot.redis;

import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.redis.spring.batch.support.RedisOperation;
import com.redis.spring.batch.support.operation.JsonSet;

import picocli.CommandLine.Command;

@Command(name = "json.set", description = "Add JSON documents to RedisJSON")
public class JsonSetCommand extends AbstractKeyCommand {

	public RedisOperation<String, String, Map<String, Object>> operation() {
		ObjectMapper mapper = new ObjectMapper();
		ObjectWriter jsonWriter = mapper.writerFor(Map.class);
		return JsonSet.<String, String, Map<String, Object>>key(key()).path("$").value(source -> {
			try {
				return jsonWriter.writeValueAsString(source);
			} catch (JsonProcessingException e) {
				throw new RuntimeException("Could not serialize to JSON", e);
			}
		}).build();
	}

}
