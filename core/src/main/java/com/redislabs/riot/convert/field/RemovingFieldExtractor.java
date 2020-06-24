package com.redislabs.riot.convert.field;

import java.util.Map;

public class RemovingFieldExtractor<K, V> extends FieldExtractor<K, V> {

    public RemovingFieldExtractor(K field) {
        super(field);
    }

    @Override
    protected V getValue(Map<K, V> source, K field) {
        return source.remove(field);
    }

}
