package com.redislabs.riot.convert.map.command;

import com.redislabs.riot.convert.KeyMaker;
import com.redislabs.riot.convert.field.FieldExtractor;
import lombok.Builder;
import org.springframework.batch.item.redis.support.commands.HmsetArgs;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public class HmsetArgsProcessor<K, V> extends KeyArgsProcessor<K, V, HmsetArgs<K, V>> {

    protected HmsetArgsProcessor(Converter<Map<K, V>, K> keyConverter) {
        super(keyConverter);
    }

    @Override
    protected HmsetArgs<K, V> process(K key, Map<K, V> item) {
        return new HmsetArgs<>(key, item);
    }

    @Builder
    private static HmsetArgsProcessor<String, String> stringBuilder(KeyMaker<Map<String, String>> keyMaker) {
        return new HmsetArgsProcessor<>(keyMaker);
    }

}
