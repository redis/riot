package com.redis.riot.core.operation;

import java.util.Map;
import java.util.function.Function;

import org.springframework.batch.item.ItemStreamException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.redis.spring.batch.writer.operation.JsonSet;

public class JsonSetBuilder extends AbstractOperationBuilder<JsonSetBuilder> {

    private final ObjectWriter jsonWriter = new ObjectMapper().writerFor(Map.class);

    private String path;

    public JsonSetBuilder path(String path) {
        this.path = path;
        return this;
    }

    @Override
    protected JsonSet<String, String, Map<String, Object>> operation() {
        return new JsonSet<String, String, Map<String, Object>>().value(this::value).path(path());
    }

    private Function<Map<String, Object>, String> path() {
        if (path == null) {
            return JsonSet.rootPath();
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
