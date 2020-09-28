package com.redislabs.riot.convert;

import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public abstract class AbstractMapConverter<K, V, T> implements Converter<Map<K, V>, T> {

    private final Converter<Map<K, V>, K> keyConverter;

    protected AbstractMapConverter(Converter<Map<K, V>, K> keyConverter) {
        this.keyConverter = keyConverter;
    }

    @Override
    public T convert(Map<K, V> source) {
        K key = keyConverter.convert(source);
        return convert(source, key);
    }

    protected abstract T convert(Map<K, V> source, K key);
}
