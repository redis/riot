package com.redislabs.riot.convert;

import org.springframework.core.convert.converter.Converter;

public class StringToDoubleConverter implements Converter<String, Double> {

    @Override
    public Double convert(String source) {
        if (source == null) {
            return null;
        }
        return Double.parseDouble(source);
    }
}
