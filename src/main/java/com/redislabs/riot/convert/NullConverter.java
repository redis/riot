package com.redislabs.riot.convert;

import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public class NullConverter<S, T> implements Converter<S, T> {
    @Override
    public T convert(S source) {
        return null;
    }
}
