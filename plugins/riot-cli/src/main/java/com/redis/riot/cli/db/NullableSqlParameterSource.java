package com.redis.riot.cli.db;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.lang.Nullable;

import java.util.Map;

public class NullableSqlParameterSource extends MapSqlParameterSource {

	public NullableSqlParameterSource() {
		super();
	}

	public NullableSqlParameterSource(String paramName, @Nullable Object value) {
		super(paramName, value);
	}

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
