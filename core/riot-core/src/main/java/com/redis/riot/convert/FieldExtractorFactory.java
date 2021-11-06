package com.redis.riot.convert;

import java.util.Map;

import org.springframework.core.convert.converter.Converter;

import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Accessors(fluent = true)
public class FieldExtractorFactory {

	private boolean remove;

	public FieldExtractorFactory remove(boolean remove) {
		this.remove = remove;
		return this;
	}

	public Converter<Map<String, Object>, Object> field(String field) {
		if (remove) {
			return s -> s.remove(field);
		}
		return s -> s.get(field);
	}

	public Converter<Map<String, Object>, String> string(String field) {
		return new CompositeConverter<>(field(field), new ObjectToStringConverter());
	}

	public <T> Converter<Map<String, T>, T> field(String field, T defaultValue) {
		if (remove) {
			return s -> s.containsKey(field) ? s.remove(field) : defaultValue;
		}
		return s -> s.containsKey(field) ? s.get(field) : defaultValue;
	}

}
