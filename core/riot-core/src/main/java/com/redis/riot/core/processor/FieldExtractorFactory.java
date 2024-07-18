package com.redis.riot.core.processor;

import java.util.Map;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;

public class FieldExtractorFactory {

	private boolean remove;

	private boolean nullCheck;

	public void setRemove(boolean remove) {
		this.remove = remove;
	}

	public void setNullCheck(boolean nullCheck) {
		this.nullCheck = nullCheck;
	}

	public Function<Map<String, Object>, Object> field(String field) {
		Function<Map<String, Object>, Object> extractor = extractor(field);
		if (nullCheck) {
			return new NullCheckExtractor(field, extractor);
		}
		return extractor;
	}

	private <T> Function<Map<String, T>, T> extractor(String field) {
		if (remove) {
			return s -> s.remove(field);
		}
		return s -> s.get(field);
	}

	public Function<Map<String, Object>, String> string(String field) {
		return field(field).andThen(new ObjectToStringFunction());
	}

	public <T> Function<Map<String, T>, T> field(String field, T defaultValue) {
		return new DefaultValueExtractor<>(extractor(field), defaultValue);
	}

	public ToLongFunction<Map<String, Object>> longField(String field) {
		Function<Map<String, Object>, Object> extractor = extractor(field);
		ObjectToLongFunction function = new ObjectToLongFunction();
		return m -> function.applyAsLong(extractor.apply(m));
	}

	public ToDoubleFunction<Map<String, Object>> doubleField(String field, double defaultValue) {
		Function<Map<String, Object>, Object> extractor = extractor(field);
		ObjectToDoubleFunction function = new ObjectToDoubleFunction(defaultValue);
		return m -> function.applyAsDouble(extractor.apply(m));
	}

	public static class MissingFieldException extends RuntimeException {

		private static final long serialVersionUID = 1L;

		public MissingFieldException(String msg) {
			super(msg);
		}

	}

	private static class DefaultValueExtractor<T> implements Function<Map<String, T>, T> {

		private final Function<Map<String, T>, T> extractor;

		private final T defaultValue;

		public DefaultValueExtractor(Function<Map<String, T>, T> extractor, T defaultValue) {
			this.extractor = extractor;
			this.defaultValue = defaultValue;
		}

		@Override
		public T apply(Map<String, T> source) {
			T value = extractor.apply(source);
			if (value == null) {
				return defaultValue;
			}
			return value;

		}

	}

	private static class NullCheckExtractor implements Function<Map<String, Object>, Object> {

		private final String field;

		private final Function<Map<String, Object>, Object> extractor;

		public NullCheckExtractor(String field, Function<Map<String, Object>, Object> extractor) {
			this.field = field;
			this.extractor = extractor;
		}

		@Override
		public Object apply(Map<String, Object> source) {
			Object value = extractor.apply(source);
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
