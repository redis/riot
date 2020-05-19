package com.redislabs.riot.convert.map;

import lombok.Builder;
import org.springframework.batch.item.redis.support.commands.XaddArgs;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public class MapToXaddConverter<K, V> extends AbstractMapConverter<K, V, XaddArgs<K, V>> {

    private final Converter<Map<K, V>, String> idConverter;
    private final Converter<Map<K, V>, Map<K, V>> fieldsConverter;

    @Builder
    protected MapToXaddConverter(Converter<Map<K, V>, K> keyConverter, Converter<Map<K, V>, String> idConverter, Converter<Map<K, V>, Map<K, V>> fieldsConverter) {
        super(keyConverter);
        this.idConverter = idConverter;
        this.fieldsConverter = fieldsConverter;
    }


    @Override
    protected XaddArgs<K, V> convert(Map<K, V> source, K key) {
        return new XaddArgs<>(key, idConverter.convert(source), fieldsConverter.convert(source));
    }
}
