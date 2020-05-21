package com.redislabs.riot.processor.command;

import lombok.Builder;
import org.springframework.batch.item.redis.support.commands.ZaddArgs;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public class ZaddArgsProcessor<K, V> extends MemberArgsProcessor<K, V> {

    private final Converter<Map<K, V>, Double> scoreConverter;

    @Builder
    public ZaddArgsProcessor(Converter<Map<K, V>, K> keyConverter, Converter<Map<K, V>, V> memberIdConverter, Converter<Map<K, V>, Double> scoreConverter) {
        super(keyConverter, memberIdConverter);
        this.scoreConverter = scoreConverter;
    }

    @Override
    protected ZaddArgs<K, V> process(K key, V memberId, Map<K, V> item) {
        Double score = scoreConverter.convert(item);
        if (score == null) {
            return null;
        }
        return new ZaddArgs<>(key, memberId, score);
    }

}
