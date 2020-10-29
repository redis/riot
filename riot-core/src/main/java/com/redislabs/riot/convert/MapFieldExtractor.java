package com.redislabs.riot.convert;

import java.util.Map;

import org.springframework.batch.item.redis.support.ConstantConverter;
import org.springframework.core.convert.converter.Converter;

import lombok.Setter;
import lombok.experimental.Accessors;

public abstract class MapFieldExtractor implements Converter<Map<String, Object>, Object> {

	protected final String field;

	protected MapFieldExtractor(String field) {
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
				return new ConstantConverter<>(defaultValue);
			}
			if (defaultValue == null) {
				if (remove) {
					return new RemovingMapFieldExtractor(field);
				}
				return new SimpleMapFieldExtractor(field);

			}
			return new DefaultingMapFieldExtractor(field, defaultValue);
		}

	}

	static class RemovingMapFieldExtractor extends MapFieldExtractor {

		protected RemovingMapFieldExtractor(String field) {
			super(field);
		}

		@Override
		protected Object getValue(Map<String, Object> source) {
			return source.remove(field);
		}

	}

	static class SimpleMapFieldExtractor extends MapFieldExtractor {

		protected SimpleMapFieldExtractor(String field) {
			super(field);
		}

		@Override
		protected Object getValue(Map<String, Object> source) {
			return source.get(field);
		}
	}

	static class DefaultingMapFieldExtractor extends SimpleMapFieldExtractor {

		private final Object defaultValue;

		protected DefaultingMapFieldExtractor(String field, Object defaultValue) {
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
