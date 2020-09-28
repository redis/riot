package com.redislabs.riot.convert;

import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public abstract class AbstractMapToCollectionConverter<K, V, T> extends AbstractMapConverter<K, V, T> {

    private final Converter<Map<K, V>, V> memberIdConverter;

    protected AbstractMapToCollectionConverter(Converter<Map<K, V>, K> keyConverter, Converter<Map<K, V>, V> memberIdConverter) {
        super(keyConverter);
        this.memberIdConverter = memberIdConverter;
    }

    @Override
    protected T convert(Map<K, V> source, K key) {
        V memberId = memberIdConverter.convert(source);
        return convert(source, key, memberId);
    }

    protected abstract T convert(Map<K, V> source, K key, V memberId);

}
