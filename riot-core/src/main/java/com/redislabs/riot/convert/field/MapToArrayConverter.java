package com.redislabs.riot.convert.field;

import lombok.Builder;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;
import java.util.function.Supplier;

public class MapToArrayConverter<K, V> implements Converter<Map<K, V>, V[]> {

    private final Converter<Map<K, V>, V>[] fieldConverters;
    private final Supplier<V[]> arraySupplier;

    public MapToArrayConverter(Converter<Map<K, V>, V>[] fieldConverters, Supplier<V[]> arraySupplier) {
        this.fieldConverters = fieldConverters;
        this.arraySupplier = arraySupplier;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
	@Builder
    private static MapToArrayConverter<String, String> stringBuilder(String... fields) {
        Converter[] extractors = new Converter[fields.length];
        for (int index = 0; index < fields.length; index++) {
            extractors[index] = new SimpleFieldExtractor<String, String>(fields[index]);
        }
        return new MapToArrayConverter<String, String>(extractors, () -> new String[fields.length]);
    }

    @Override
    public V[] convert(Map<K, V> source) {
        V[] array = arraySupplier.get();
        for (int index = 0; index < fieldConverters.length; index++) {
            array[index] = fieldConverters[index].convert(source);
        }
        return array;
    }
}
