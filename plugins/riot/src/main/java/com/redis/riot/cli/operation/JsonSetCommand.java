package com.redis.riot.cli.operation;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.springframework.batch.item.ItemStreamException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.redis.spring.batch.writer.operation.JsonSet;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "json.set", description = "Add JSON documents to RedisJSON")
public class JsonSetCommand extends AbstractKeyCommand {

	@Option(names = "--path", description = "Path field.", paramLabel = "<field>")
	private Optional<String> path = Optional.empty();

	private ObjectMapper mapper = new ObjectMapper();
	private ObjectWriter jsonWriter = mapper.writerFor(Map.class);

	public JsonSet<String, String, Map<String, Object>> operation() {
		return new JsonSet<>(key(), this::value, path());
	}

	private Function<Map<String, Object>, String> path() {
		return path.map(this::stringFieldExtractor).orElse(JsonSet.rootPath());
	}

	private String value(Map<String, Object> map) {
		try {
			return jsonWriter.writeValueAsString(map);
		} catch (JsonProcessingException e) {
			throw new ItemStreamException("Could not serialize to JSON", e);
		}
	}

}
