package com.redis.riot;

import java.util.Map;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.lang.Nullable;

public class NullableSqlParameterSource extends MapSqlParameterSource {

	public NullableSqlParameterSource(@Nullable Map<String, ?> values) {
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