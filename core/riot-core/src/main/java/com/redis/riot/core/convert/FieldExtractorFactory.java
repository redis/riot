package com.redis.riot.core.convert;

import java.util.Map;

import org.springframework.core.convert.converter.Converter;

public class FieldExtractorFactory {

	private boolean remove;
	private boolean nullCheck;

	public void setRemove(boolean remove) {
		this.remove = remove;
	}

	public void setNullCheck(boolean nullCheck) {
		this.nullCheck = nullCheck;
	}

	public Converter<Map<String, Object>, Object> field(String field) {
		Converter<Map<String, Object>, Object> extractor = extractor(field);
		if (nullCheck) {
			return new NullCheckExtractor(field, extractor);
		}
		return extractor;
	}

	private <T> Converter<Map<String, T>, T> extractor(String field) {
		if (remove) {
			return s -> s.remove(field);
		}
		return s -> s.get(field);
	}

	public Converter<Map<String, Object>, String> string(String field) {
		return new CompositeConverter<>(field(field), new ObjectToStringConverter());
	}

	public <T> Converter<Map<String, T>, T> field(String field, T defaultValue) {
		return new DefaultValueExtractor<>(extractor(field), defaultValue);
	}

	public static class MissingFieldException extends RuntimeException {

		private static final long serialVersionUID = 1L;

		public MissingFieldException(String msg) {
			super(msg);
		}

	}

	private static class DefaultValueExtractor<T> implements Converter<Map<String, T>, T> {

		private final Converter<Map<String, T>, T> extractor;
		private final T defaultValue;

		public DefaultValueExtractor(Converter<Map<String, T>, T> extractor, T defaultValue) {
			this.extractor = extractor;
			this.defaultValue = defaultValue;
		}

		@Override
		public T convert(Map<String, T> source) {
			T value = extractor.convert(source);
			if (value == null) {
				return defaultValue;
			}
			return value;

		}
	}

	private static class NullCheckExtractor implements Converter<Map<String, Object>, Object> {

		private final String field;
		private final Converter<Map<String, Object>, Object> extractor;

		public NullCheckExtractor(String field, Converter<Map<String, Object>, Object> extractor) {
			this.field = field;
			this.extractor = extractor;
		}

		@Override
		public Object convert(Map<String, Object> source) {
			Object value = extractor.convert(source);
			if (value == null) {
				throw new MissingFieldException("Error: Missing required field: '" + field + "'");
			}
			return value;
		}

	}

	public static FieldExtractorFactoryBuilder builder() {
		return new FieldExtractorFactoryBuilder();
	}

	public static class FieldExtractorFactoryBuilder {

		private boolean remove;
		private boolean nullCheck;

		public FieldExtractorFactoryBuilder remove(boolean remove) {
			this.remove = remove;
			return this;
		}

		public FieldExtractorFactoryBuilder nullCheck(boolean nullCheck) {
			this.nullCheck = nullCheck;
			return this;
		}

		public FieldExtractorFactory build() {
			FieldExtractorFactory factory = new FieldExtractorFactory();
			factory.setRemove(remove);
			factory.setNullCheck(nullCheck);
			return factory;
		}
	}

}
