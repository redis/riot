package com.redis.riot.core.function;

import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.UnaryOperator;

public class MapFunction implements UnaryOperator<Map<String, Object>> {

    private final Map<String, Function<Map<String, Object>, Object>> fields;

    public MapFunction(Map<String, Function<Map<String, Object>, Object>> fields) {
        this.fields = fields;
    }

    @Override
    public Map<String, Object> apply(Map<String, Object> t) {
        for (Entry<String, Function<Map<String, Object>, Object>> field : fields.entrySet()) {
            t.put(field.getKey(), field.getValue().apply(t));
        }
        return t;
    }

}
