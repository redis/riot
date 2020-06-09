package com.redislabs.riot.processor;

import com.redislabs.riot.convert.map.RegexNamedGroupsExtractor;
import lombok.Builder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.converter.Converter;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

public class MapProcessor<K, V> implements ItemProcessor<Map<K, V>, Map<K, V>> {

    private final Map<K, Converter<V, Map<K, V>>> extractors;

    public MapProcessor(Map<K, Converter<V, Map<K, V>>> extractors) {
        this.extractors = extractors;
    }

    @Override
    public Map<K, V> process(Map<K, V> item) {
        for (K field : extractors.keySet()) {
            V value = item.get(field);
            if (value == null) {
                continue;
            }
            item.putAll(extractors.get(field).convert(value));
        }
        return item;
    }

    @Builder
    private static MapProcessor<String, String> build(Map<String, String> regexes) {
        Map<String, Converter<String, Map<String, String>>> extractors = new LinkedHashMap<>();
        for (String field : regexes.keySet()) {
            extractors.put(field, RegexNamedGroupsExtractor.builder().regex(regexes.get(field)).build());
        }
        return new MapProcessor<>(extractors);
    }

}
