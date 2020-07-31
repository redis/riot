package com.redislabs.riot.convert;

import org.springframework.core.convert.converter.Converter;

public class StringToLongConverter implements Converter<String, Long> {

    @Override
    public Long convert(String source) {
        if (source == null) {
            return null;
        }
        return Long.parseLong(source);
    }
}
