package com.redislabs.riot.convert;

import org.springframework.core.convert.converter.Converter;

public class NullConverter<S, T> implements Converter<S, T> {
    @Override
    public T convert(S source) {
        return null;
    }
}
