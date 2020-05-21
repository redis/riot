package com.redislabs.riot.processor;

import lombok.Builder;
import lombok.Singular;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;
import java.util.Map.Entry;

@Builder
public class MapProcessor<K, V> implements ItemProcessor<Map<K, V>, Map<K, V>> {

    @Singular
    private final Map<K, Converter<V, Map<K, V>>> extractors;

    @Override
    public Map<K, V> process(Map<K, V> item) {
        for (Entry<K, Converter<V, Map<K, V>>> extractor : extractors.entrySet()) {
            item.putAll(extractor.getValue().convert(item.get(extractor.getKey())));
        }
        return item;
    }

}
