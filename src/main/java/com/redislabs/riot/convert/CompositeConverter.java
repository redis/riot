package com.redislabs.riot.convert;

import org.springframework.core.convert.converter.Converter;

public class CompositeConverter<S, M, T> implements Converter<S, T> {

    private final Converter<S, M> fromConverter;
    private final Converter<M, T> toConverter;

    public CompositeConverter(Converter<S, M> fromConverter, Converter<M, T> toConverter) {
        this.fromConverter = fromConverter;
        this.toConverter = toConverter;
    }

    @Override
    public T convert(S source) {
        M pivot = fromConverter.convert(source);
        if (pivot == null) {
            return null;
        }
        return toConverter.convert(pivot);
    }
}
