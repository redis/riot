package com.redis.riot.core.function;

import java.util.function.ToDoubleFunction;

public class ObjectToDoubleFunction implements ToDoubleFunction<Object> {

    private final double defaultValue;

    public ObjectToDoubleFunction(double defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Override
    public double applyAsDouble(Object value) {
        if (value != null) {
            if (value instanceof Number) {
                return ((Number) value).doubleValue();
            }
            if (value instanceof String) {
                String string = (String) value;
                if (!string.isEmpty()) {
                    return Double.parseDouble(string);
                }
            }
        }
        return defaultValue;
    }

}
