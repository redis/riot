package com.redis.riot.convert;

import java.util.Map;

public class RemovingFieldExtractor extends FieldExtractor {

    protected RemovingFieldExtractor(String field) {
        super(field);
    }

    @Override
    protected Object getValue(Map<String, Object> source) {
        return source.remove(field);
    }

}