package com.redislabs.riot.processor.command;

import lombok.Builder;
import org.springframework.batch.item.redis.support.commands.SetArgs;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public class SetArgsProcessor<K, V> extends KeyArgsProcessor<K, V, SetArgs<K, V>> {

    private final Converter<Map<K, V>, V> valueConverter;

    @Builder
    public SetArgsProcessor(Converter<Map<K, V>, K> keyConverter, Converter<Map<K, V>, V> valueConverter) {
        super(keyConverter);
        this.valueConverter = valueConverter;
    }

    @Override
    protected SetArgs<K, V> process(K key, Map<K, V> item) {
        return new SetArgs<>(key, valueConverter.convert(item));
    }

}
