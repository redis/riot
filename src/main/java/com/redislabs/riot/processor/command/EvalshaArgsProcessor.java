package com.redislabs.riot.processor.command;

import lombok.Builder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.redis.support.commands.EvalshaArgs;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public class EvalshaArgsProcessor<K, V> implements ItemProcessor<Map<K, V>, EvalshaArgs<K, V>> {

    private final Converter<Map<K, V>, K[]> keysConverter;
    private final Converter<Map<K, V>, V[]> argsConverter;

    @Builder
    public EvalshaArgsProcessor(Converter<Map<K, V>, K[]> keysConverter, Converter<Map<K, V>, V[]> argsConverter) {
        this.keysConverter = keysConverter;
        this.argsConverter = argsConverter;
    }

    @Override
    public EvalshaArgs<K, V> process(Map<K, V> item) {
        return new EvalshaArgs<>(keysConverter.convert(item), argsConverter.convert(item));
    }

}
