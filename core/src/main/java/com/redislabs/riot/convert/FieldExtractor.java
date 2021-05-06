package com.redislabs.riot.convert;

import java.util.Map;

import org.springframework.core.convert.converter.Converter;

import lombok.Setter;
import lombok.experimental.Accessors;

public abstract class FieldExtractor implements Converter<Map<String, Object>, Object> {

	protected final String field;

	protected FieldExtractor(String field) {
		this.field = field;
	}

	@Override
	public Object convert(Map<String, Object> source) {
		return getValue(source);
	}

	abstract protected Object getValue(Map<String, Object> source);

	public static FieldExtractorBuilder builder() {
		return new FieldExtractorBuilder();
	}

	@Setter
	@Accessors(fluent = true)
	public static class FieldExtractorBuilder {

		private String field;
		private boolean remove;
		private Object defaultValue;

		public Converter<Map<String, Object>, Object> build() {
			if (field == null) {
				if (defaultValue == null) {
					return null;
				}
				return s -> defaultValue;
			}
			if (defaultValue == null) {
				if (remove) {
					return new RemovingFieldExtractor(field);
				}
				return new SimpleFieldExtractor(field);

			}
			return new DefaultingFieldExtractor(field, defaultValue);
		}

	}

}
