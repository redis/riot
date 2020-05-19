package com.redislabs.riot.convert.field;

import lombok.Builder;

import java.util.Map;

public class SimpleFieldExtractor<K, V> extends FieldExtractor<K, V> {

    public SimpleFieldExtractor(K field) {
        super(field);
    }

    @Override
    protected V getValue(Map<K, V> source, K field) {
        return source.get(field);
    }
}
