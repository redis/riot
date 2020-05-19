package com.redislabs.riot.convert;

import org.springframework.core.convert.converter.Converter;

public class IdemConverter<S, T> implements Converter<S, T> {

    @Override
    public T convert(S source) {
        return (T) source;
    }
}
