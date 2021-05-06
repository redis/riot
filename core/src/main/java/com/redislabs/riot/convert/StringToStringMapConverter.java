package com.redislabs.riot.convert;

import lombok.Data;
import org.springframework.core.convert.converter.Converter;

import java.util.HashMap;
import java.util.Map;

@Data
public class StringToStringMapConverter implements Converter<String, Map<String, String>> {

    public static final Converter<String, String> DEFAULT_KEY_EXTRACTOR = s -> "value";

    private Converter<String, String> keyExtractor = DEFAULT_KEY_EXTRACTOR;

    @Override
    public Map<String, String> convert(String source) {
        Map<String, String> result = new HashMap<>();
        result.put(keyExtractor.convert(source), source);
        return result;
    }

}
