package com.redislabs.riot.processor.command;

import org.springframework.batch.item.redis.support.commands.MemberArgs;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public class MemberArgsProcessor<K, V> extends KeyArgsProcessor<K, V, MemberArgs<K, V>> {

    private final Converter<Map<K, V>, V> memberIdConverter;

    public MemberArgsProcessor(Converter<Map<K, V>, K> keyConverter, Converter<Map<K, V>, V> memberIdConverter) {
        super(keyConverter);
        this.memberIdConverter = memberIdConverter;
    }

    @Override
    protected MemberArgs<K, V> process(K key, Map<K, V> item) {
        return process(key, memberIdConverter.convert(item), item);
    }

    protected MemberArgs<K, V> process(K key, V memberId, Map<K, V> item) {
        return new MemberArgs<>(key, memberId);
    }

}
