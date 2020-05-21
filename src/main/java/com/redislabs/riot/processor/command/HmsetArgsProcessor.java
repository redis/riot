package com.redislabs.riot.processor.command;

import lombok.Builder;
import org.springframework.batch.item.redis.support.commands.HmsetArgs;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public class HmsetArgsProcessor<K, V> extends KeyArgsProcessor<K, V, HmsetArgs<K, V>> {

    @Builder
    public HmsetArgsProcessor(Converter<Map<K, V>, K> keyConverter) {
        super(keyConverter);
    }

    @Override
    protected HmsetArgs<K, V> process(K key, Map<K, V> item) {
        return new HmsetArgs<>(key, item);
    }


}
