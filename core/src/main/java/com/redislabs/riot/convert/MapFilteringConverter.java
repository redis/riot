package com.redislabs.riot.convert;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.*;

public class MapFilteringConverter<K, V> implements Converter<Map<K, V>, Map<K, V>> {

    private final Set<K> includes;
    private final Set<K> excludes;

    public MapFilteringConverter(Set<K> includes, Set<K> excludes) {
        this.includes = includes;
        this.excludes = excludes;
    }

    @Override
    public Map<K, V> convert(Map<K, V> source) {
        Map<K, V> filtered = includes.isEmpty() ? source : new LinkedHashMap<>();
        includes.forEach(f -> filtered.put(f, source.get(f)));
        excludes.forEach(filtered::remove);
        return filtered;
    }

    public static <K, V> MapFilteringConverterBuilder<K, V> builder() {
        return new MapFilteringConverterBuilder();
    }

    public static class MapFilteringConverterBuilder<K, V> {

        private List<K> includes = new ArrayList<>();
        private List<K> excludes = new ArrayList<>();

        public MapFilteringConverterBuilder includes(K... fields) {
            Assert.notNull(fields, "Fields cannot be null");
            this.includes = Arrays.asList(fields);
            return this;
        }

        public MapFilteringConverterBuilder excludes(K... fields) {
            Assert.notNull(fields, "Fields cannot be null");
            this.excludes = Arrays.asList(fields);
            return this;
        }

        public MapFilteringConverter build() {
            return new MapFilteringConverter(new LinkedHashSet<>(includes), new LinkedHashSet<>(excludes));
        }

    }
}
