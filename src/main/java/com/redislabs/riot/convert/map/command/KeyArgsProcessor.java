package com.redislabs.riot.convert.map.command;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public abstract class KeyArgsProcessor<K, V, T> implements ItemProcessor<Map<K, V>, T> {

    private final Converter<Map<K, V>, K> keyConverter;

    protected KeyArgsProcessor(Converter<Map<K, V>, K> keyConverter) {
        this.keyConverter = keyConverter;
    }

    @Override
    public T process(Map<K, V> item) throws Exception {
        return process(keyConverter.convert(item), item);
    }

    protected abstract T process(K key, Map<K, V> item);
}
