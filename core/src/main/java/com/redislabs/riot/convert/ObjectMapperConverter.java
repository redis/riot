package com.redislabs.riot.convert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.convert.converter.Converter;

@Slf4j
public class ObjectMapperConverter<T> implements Converter<T, String> {

    private final ObjectWriter writer;

    public ObjectMapperConverter(ObjectWriter writer) {
        this.writer = writer;
    }

    @Override
    public String convert(T source) {
        try {
            return writer.writeValueAsString(source);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not convert object to XML", e);
        }
    }
}
