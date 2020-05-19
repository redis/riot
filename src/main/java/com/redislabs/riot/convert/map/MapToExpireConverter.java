package com.redislabs.riot.convert.map;

import org.springframework.batch.item.redis.support.commands.PexpireArgs;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public class MapToExpireConverter<K, V> extends AbstractMapConverter<K, V, PexpireArgs<K>> {

    private Converter<Map<K, V>, Long> timeoutConverter;

    protected MapToExpireConverter(Converter<Map<K, V>, K> keyConverter, Converter<Map<K, V>, Long> timeoutConverter) {
        super(keyConverter);
        this.timeoutConverter = timeoutConverter;
    }

    @Override
    protected PexpireArgs<K> convert(Map<K, V> source, K key) {
        return new PexpireArgs<>(key, timeoutConverter.convert(source));
    }
}