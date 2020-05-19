package com.redislabs.riot.processor;

import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.converter.Converter;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class MapFlattener<K, V> implements ItemProcessor<Map<K, Object>, Map<K, V>> {

    private final BiFunction<K, K, K> nestedKeyFunction;
    private final BiFunction<K, Integer, K> indexedKeyFunction;
    private final Converter<Object, V> valueConverter;

    public MapFlattener(BiFunction<K, K, K> nestedKeyFunction, BiFunction<K, Integer, K> indexedKeyFunction, Converter<Object, V> valueConverter) {
        this.nestedKeyFunction = nestedKeyFunction;
        this.indexedKeyFunction = indexedKeyFunction;
        this.valueConverter = valueConverter;
    }

    @Override
    public Map<K, V> process(Map<K, Object> item) {
        Map<K, V> flatMap = new HashMap<>();
        for (Map.Entry<K, Object> entry : item.entrySet()) {
            process(flatMap, entry.getKey(), entry.getValue());
        }
        return flatMap;
    }

    private void process(Map<K, V> stringMap, K key, Object value) {
        if (value instanceof Map) {
            Map<K, Object> map = (Map<K, Object>) value;
            for (Map.Entry<K, Object> entry : map.entrySet()) {
                process(stringMap, nestedKeyFunction.apply(key, entry.getKey()), entry.getValue());
            }
            return;
        }
        if (value instanceof Collection) {
            Collection<Object> collection = (Collection<Object>) value;
            int index = 0;
            for (Object element : collection) {
                process(stringMap, indexedKeyFunction.apply(key, index), element);
                index++;
            }
            return;
        }
        stringMap.put(key, valueConverter.convert(value));
    }

    public static MapFlattenerBuilder builder() {
        return new MapFlattenerBuilder();
    }

    @Accessors(fluent = true)
    @Setter
    public static class MapFlattenerBuilder {

        private static final String DEFAULT_NESTED_KEY_SEPARATOR = ".";
        private static final String DEFAULT_INDEXED_KEY_FORMAT = "%s[%s]";

        private String nestedKeySeparator = DEFAULT_NESTED_KEY_SEPARATOR;
        private String indexedKeyFormat = DEFAULT_INDEXED_KEY_FORMAT;

        public MapFlattener<String, String> build() {
            return new MapFlattener((k1, k2) -> k1 + nestedKeySeparator + k2, (k, i) -> String.format(indexedKeyFormat, k, i), o -> String.valueOf(o));
        }

    }

}
