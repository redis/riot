package com.redislabs.riot.convert;

import org.springframework.core.convert.converter.Converter;

public class StringToIntegerConverter implements Converter<String, Integer> {

    @Override
    public Integer convert(String source) {
        if (source == null) {
            return null;
        }
        return Integer.parseInt(source);
    }
}