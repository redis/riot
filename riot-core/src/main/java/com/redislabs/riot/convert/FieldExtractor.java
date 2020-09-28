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

	@Accessors(fluent = true)
	@Setter
	public static class FieldExtractorBuilder {

		private String field;
		private boolean remove;
		private Object defaultValue;

		public Converter<Map<String, Object>, Object> build() {
			if (field == null) {
				if (defaultValue == null) {
					return null;
				}
				return new ConstantFieldExtractor(defaultValue);
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

	static class RemovingFieldExtractor extends FieldExtractor {

		protected RemovingFieldExtractor(String field) {
			super(field);
		}

		@Override
		protected Object getValue(Map<String, Object> source) {
			return source.remove(field);
		}

	}

	static class SimpleFieldExtractor extends FieldExtractor {

		protected SimpleFieldExtractor(String field) {
			super(field);
		}

		@Override
		protected Object getValue(Map<String, Object> source) {
			return source.get(field);
		}
	}

	static class DefaultingFieldExtractor extends SimpleFieldExtractor {

		private final Object defaultValue;

		protected DefaultingFieldExtractor(String field, Object defaultValue) {
			super(field);
			this.defaultValue = defaultValue;
		}

		@Override
		public Object convert(Map<String, Object> source) {
			if (source.containsKey(field)) {
				return super.convert(source);
			}
			return defaultValue;
		}

	}
}
