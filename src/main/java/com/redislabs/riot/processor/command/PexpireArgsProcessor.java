package com.redislabs.riot.processor.command;

import lombok.Builder;
import org.springframework.batch.item.redis.support.commands.PexpireArgs;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public class PexpireArgsProcessor<K, V> extends KeyArgsProcessor<K, V, PexpireArgs<K>> {

    private final Converter<Map<K, V>, Long> timeoutConverter;

    @Builder
    public PexpireArgsProcessor(Converter<Map<K, V>, K> keyConverter, Converter<Map<K, V>, Long> timeoutConverter) {
        super(keyConverter);
        this.timeoutConverter = timeoutConverter;
    }

    @Override
    protected PexpireArgs<K> process(K key, Map<K, V> item) {
        Long timeout = timeoutConverter.convert(item);
        if (timeout == null) {
            return null;
        }
        return new PexpireArgs<>(key, timeout);
    }

}
