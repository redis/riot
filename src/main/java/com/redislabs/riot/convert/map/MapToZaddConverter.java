package com.redislabs.riot.convert.map;

import lombok.Builder;
import org.springframework.batch.item.redis.support.commands.ZaddArgs;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public class MapToZaddConverter<K, V> extends AbstractMapToCollectionConverter<K, V, ZaddArgs<K, V>> {

    private final Converter<Map<K, V>, Long> scoreConverter;

    @Builder
    protected MapToZaddConverter(Converter<Map<K, V>, K> keyConverter, Converter<Map<K, V>, V> memberIdConverter, Converter<Map<K, V>, Long> scoreConverter) {
        super(keyConverter, memberIdConverter);
        this.scoreConverter = scoreConverter;
    }

    @Override
    protected ZaddArgs<K, V> convert(Map<K, V> source, K key, V memberId) {
        return new ZaddArgs<>(key, memberId, scoreConverter.convert(source));
    }

}
