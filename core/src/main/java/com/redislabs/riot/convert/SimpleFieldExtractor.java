package com.redislabs.riot.convert;

import java.util.Map;

public class SimpleFieldExtractor extends FieldExtractor {

    protected SimpleFieldExtractor(String field) {
        super(field);
    }

    @Override
    protected Object getValue(Map<String, Object> source) {
        return source.get(field);
    }

}