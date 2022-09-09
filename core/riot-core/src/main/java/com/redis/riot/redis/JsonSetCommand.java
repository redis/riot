package com.redis.riot.redis;

import java.util.Map;

import org.springframework.batch.item.ItemStreamException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.redis.spring.batch.writer.operation.JsonSet;

import picocli.CommandLine.Command;

@Command(name = "json.set", description = "Add JSON documents to RedisJSON")
public class JsonSetCommand extends AbstractKeyCommand {

	public JsonSet<String, String, Map<String, Object>> operation() {
		ObjectMapper mapper = new ObjectMapper();
		ObjectWriter jsonWriter = mapper.writerFor(Map.class);
		return JsonSet.<String, Map<String, Object>>key(key()).value(source -> {
			try {
				return jsonWriter.writeValueAsString(source);
			} catch (JsonProcessingException e) {
				throw new ItemStreamException("Could not serialize to JSON", e);
			}
		}).build();
	}

}
