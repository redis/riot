package com.redislabs.riot.convert.map;

import org.springframework.batch.item.redis.support.commands.MemberArgs;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public class MapToMemberConverter<K, V> extends AbstractMapToCollectionConverter<K, V, MemberArgs<K, V>> {

    public MapToMemberConverter(Converter<Map<K, V>, K> keyConverter, Converter<Map<K, V>, V> memberIdConverter) {
        super(keyConverter, memberIdConverter);
    }

    @Override
    protected MemberArgs<K, V> convert(Map<K, V> source, K key, V memberId) {
        return new MemberArgs<>(key, memberId);
    }
}
