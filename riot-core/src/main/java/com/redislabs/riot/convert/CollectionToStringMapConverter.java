package com.redislabs.riot.convert;

import lombok.Builder;
import org.springframework.core.convert.converter.Converter;

import java.util.*;

@Builder
public class CollectionToStringMapConverter<S extends Collection<String>> implements Converter<S, Map<String, String>> {

    public static final String DEFAULT_KEY_FORMAT = "[%s]";

    @Builder.Default
    private final String keyFormat = DEFAULT_KEY_FORMAT;

    @Override
    public Map<String, String> convert(S source) {
        Map<String, String> result = new HashMap<>();
        int index = 0;
        for (String element : source) {
            result.put(String.format(keyFormat, index), element);
            index++;
        }
        return result;
    }
}
