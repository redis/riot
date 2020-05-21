package com.redislabs.riot.convert.field;

import org.springframework.core.convert.converter.Converter;

public class ConstantFieldExtractor<S, T> implements Converter<S, T> {

    private final T constant;

    public ConstantFieldExtractor(T constant) {
        this.constant = constant;
    }

    @Override
    public T convert(S source) {
        return constant;
    }
}
