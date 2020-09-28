package com.redislabs.riot.convert;

import org.springframework.core.convert.converter.Converter;

public class DefaultingCompositeConverter<S, M, T> implements Converter<S, T> {

    private final Converter<S, M> fromConverter;
    private final T defaultValue;
    private final Converter<M, T> toConverter;

    public DefaultingCompositeConverter(Converter<S, M> fromConverter, T defaultValue, Converter<M, T> toConverter) {
        this.fromConverter = fromConverter;
        this.defaultValue = defaultValue;
        this.toConverter = toConverter;
    }

    @Override
    public T convert(S source) {
        M pivot = fromConverter.convert(source);
        if (pivot == null) {
            return defaultValue;
        }
        return toConverter.convert(pivot);
    }
}
