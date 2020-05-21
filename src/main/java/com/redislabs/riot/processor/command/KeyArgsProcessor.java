package com.redislabs.riot.processor.command;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.redis.support.commands.KeyArgs;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public abstract class KeyArgsProcessor<K, V, T extends KeyArgs<K>> implements ItemProcessor<Map<K, V>, T> {

    private final Converter<Map<K, V>, K> keyConverter;

    protected KeyArgsProcessor(Converter<Map<K, V>, K> keyConverter) {
        this.keyConverter = keyConverter;
    }

    @Override
    public T process(Map<K, V> item) {
        return process(keyConverter.convert(item), item);
    }

    protected abstract T process(K key, Map<K, V> item);
}
