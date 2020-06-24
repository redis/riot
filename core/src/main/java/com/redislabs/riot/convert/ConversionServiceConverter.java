package com.redislabs.riot.convert;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.DefaultConversionService;

public class ConversionServiceConverter<S, T> implements Converter<S, T> {

    private final ConversionService conversionService;
    private final Class<T> targetType;

    public ConversionServiceConverter(ConversionService conversionService, Class<T> targetType) {
        this.conversionService = conversionService;
        this.targetType = targetType;
    }

    public ConversionServiceConverter(Class<T> targetType) {
        this(new DefaultConversionService(), targetType);
    }

    @Override
    public T convert(S source) {
        return conversionService.convert(source, targetType);
    }

}
