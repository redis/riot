package com.redislabs.riot.convert;

import java.util.Map;

import org.springframework.core.convert.converter.Converter;

public class MapToStringArrayConverter implements Converter<Map<String, Object>, String[]> {

    private final Converter<Map<String, Object>, String>[] fieldConverters;

    public MapToStringArrayConverter(Converter<Map<String, Object>, String>[] fieldConverters) {
        this.fieldConverters = fieldConverters;
    }

    @Override
    public String[] convert(Map<String, Object> source) {
        String[] array = new String[fieldConverters.length];
        for (int index = 0; index < fieldConverters.length; index++) {
            array[index] = fieldConverters[index].convert(source);
        }
        return array;
    }

}
