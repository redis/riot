package com.redislabs.riot.convert.field;

import java.util.Map;

public class DefaultingFieldExtractor<K, V> extends FieldExtractor<K, V> {

    private final V defaultValue;

    protected DefaultingFieldExtractor(K field, V defaultValue) {
        super(field);
        this.defaultValue = defaultValue;
    }

    @Override
    protected V getValue(Map<K, V> source, K field) {
        return source.getOrDefault(field, defaultValue);
    }
}
