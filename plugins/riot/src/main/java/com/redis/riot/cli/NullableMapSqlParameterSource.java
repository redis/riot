package com.redis.riot.cli;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.lang.Nullable;

import java.util.Map;

public class NullableMapSqlParameterSource extends MapSqlParameterSource {

    public NullableMapSqlParameterSource() {
        super();
    }

    public NullableMapSqlParameterSource(String paramName, @Nullable Object value) {
        super(paramName, value);
    }

    public NullableMapSqlParameterSource(@Nullable Map<String, ?> values) {
        super(values);
    }

    @Override
    @Nullable
    public Object getValue(String paramName) {
        if (!hasValue(paramName)) {
            return null;
        }
        return super.getValue(paramName);
    }


}
