package com.redis.riot.convert;

import java.util.Map;

public class DefaultingFieldExtractor extends SimpleFieldExtractor {

    private final Object defaultValue;

    protected DefaultingFieldExtractor(String field, Object defaultValue) {
        super(field);
        this.defaultValue = defaultValue;
    }

    @Override
    public Object convert(Map<String, Object> source) {
        if (source.containsKey(field)) {
            return super.convert(source);
        }
        return defaultValue;
    }

}