package com.redislabs.riot.processor.command;

import lombok.Builder;
import org.springframework.batch.item.redis.support.commands.XaddArgs;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public class XaddArgsProcessor<K, V> extends KeyArgsProcessor<K, V, XaddArgs<K, V>> {

    private final Converter<Map<K, V>, String> idConverter;

    @Builder
    public XaddArgsProcessor(Converter<Map<K, V>, K> keyConverter, Converter<Map<K, V>, String> idConverter) {
        super(keyConverter);
        this.idConverter = idConverter;
    }

    @Override
    protected XaddArgs<K, V> process(K key, Map<K, V> item) {
        return new XaddArgs<>(key, idConverter.convert(item), item);
    }

}
