package com.redis.riot.core.convert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class IdConverterBuilder {

	public static final String DEFAULT_SEPARATOR = ":";

	private String separator = DEFAULT_SEPARATOR;
	private Optional<String> prefix = Optional.empty();
	private final FieldExtractorFactory extractorFactory = FieldExtractorFactory.builder().nullCheck(true).build();
	private final List<String> fields = new ArrayList<>();

	public IdConverterBuilder remove(boolean remove) {
		this.extractorFactory.setRemove(remove);
		return this;
	}

	public IdConverterBuilder fields(String... fields) {
		if (fields != null) {
			for (String field : fields) {
				this.fields.add(field);
			}
		}
		return this;
	}

	public IdConverterBuilder prefix(String prefix) {
		return prefix(Optional.of(prefix));
	}

	public IdConverterBuilder prefix(Optional<String> prefix) {
		this.prefix = prefix;
		return this;
	}

	public IdConverterBuilder separator(String separator) {
		this.separator = separator;
		return this;
	}

	public Function<Map<String, Object>, String> build() {
		if (fields.isEmpty()) {
			if (prefix.isPresent()) {
				return m -> prefix.get();
			}
			throw new IllegalArgumentException("No prefix and no fields specified");
		}
		if (fields.size() == 1) {
			Function<Map<String, Object>, String> extractor = extractorFactory.string(fields.get(0));
			if (prefix.isPresent()) {
				return s -> prefix.get() + separator + extractor.apply(s);
			}
			return extractor::apply;
		}
		List<Function<Map<String, Object>, String>> stringConverters = new ArrayList<>();
		prefix.ifPresent(p -> stringConverters.add(s -> p));
		for (String field : fields) {
			stringConverters.add(extractorFactory.string(field));
		}
		return new ConcatenatingConverter(separator, stringConverters);
	}

	public static class ConcatenatingConverter implements Function<Map<String, Object>, String> {

		private final String separator;
		private final List<Function<Map<String, Object>, String>> converters;

		public ConcatenatingConverter(String separator, List<Function<Map<String, Object>, String>> stringConverters) {
			this.separator = separator;
			this.converters = stringConverters;
		}

		@Override
		public String apply(Map<String, Object> source) {
			if (source == null) {
				return null;
			}
			StringBuilder builder = new StringBuilder();
			builder.append(converters.get(0).apply(source));
			for (int index = 1; index < converters.size(); index++) {
				builder.append(separator);
				builder.append(converters.get(index).apply(source));
			}
			return builder.toString();
		}

	}

}
