package com.redis.riot.core.operation;

import java.util.Map;
import java.util.function.Function;

import org.springframework.batch.item.ItemStreamException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.redis.spring.batch.writer.operation.AbstractKeyWriteOperation;
import com.redis.spring.batch.writer.operation.JsonSet;

public class JsonSetBuilder extends AbstractMapOperationBuilder {

	private final ObjectWriter jsonWriter = new ObjectMapper().writerFor(Map.class);

	private String path;

	public void setPath(String path) {
		this.path = path;
	}

	@Override
	protected AbstractKeyWriteOperation<String, String, Map<String, Object>> operation(
			Function<Map<String, Object>, String> keyFunction) {
		JsonSet<String, String, Map<String, Object>> operation = new JsonSet<>(keyFunction, this::value);
		operation.setPathFunction(path());
		return operation;
	}

	private Function<Map<String, Object>, String> path() {
		if (path == null) {
			return t -> JsonSet.ROOT_PATH;
		}
		return toString(path);
	}

	private String value(Map<String, Object> map) {
		try {
			return jsonWriter.writeValueAsString(map);
		} catch (JsonProcessingException e) {
			throw new ItemStreamException("Could not serialize to JSON", e);
		}
	}

}
