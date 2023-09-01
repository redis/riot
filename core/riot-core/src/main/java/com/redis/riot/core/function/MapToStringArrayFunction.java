package com.redis.riot.core.function;

import java.util.Map;
import java.util.function.Function;

public class MapToStringArrayFunction implements Function<Map<String, Object>, String[]> {

    private final Function<Map<String, Object>, String>[] fieldFunctions;

    public MapToStringArrayFunction(Function<Map<String, Object>, String>[] fieldFunctions) {
        this.fieldFunctions = fieldFunctions;
    }

    @Override
    public String[] apply(Map<String, Object> source) {
        String[] array = new String[fieldFunctions.length];
        for (int index = 0; index < fieldFunctions.length; index++) {
            array[index] = fieldFunctions[index].apply(source);
        }
        return array;
    }

}
