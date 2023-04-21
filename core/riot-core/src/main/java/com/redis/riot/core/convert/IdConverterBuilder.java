package com.redis.riot.core.convert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.core.convert.converter.Converter;

public class IdConverterBuilder {

	public static final String DEFAULT_SEPARATOR = ":";

	private String separator = DEFAULT_SEPARATOR;
	private String prefix;
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
		this.prefix = prefix;
		return this;
	}

	public IdConverterBuilder separator(String separator) {
		this.separator = separator;
		return this;
	}

	public Converter<Map<String, Object>, String> build() {
		if (fields.isEmpty()) {
			if (prefix == null) {
				throw new IllegalArgumentException("No prefix and no fields specified");
			}
			return s -> prefix;
		}
		if (fields.size() == 1) {
			Converter<Map<String, Object>, String> extractor = extractorFactory.string(fields.get(0));
			if (prefix == null) {
				return extractor::convert;
			}
			return s -> prefix + separator + extractor.convert(s);
		}
		List<Converter<Map<String, Object>, String>> stringConverters = new ArrayList<>();
		if (prefix != null) {
			stringConverters.add(s -> prefix);
		}
		for (String field : fields) {
			stringConverters.add(extractorFactory.string(field));
		}
		return new ConcatenatingConverter(separator, stringConverters);
	}

	public static class ConcatenatingConverter implements Converter<Map<String, Object>, String> {

		private final String separator;
		private final List<Converter<Map<String, Object>, String>> converters;

		public ConcatenatingConverter(String separator, List<Converter<Map<String, Object>, String>> stringConverters) {
			this.separator = separator;
			this.converters = stringConverters;
		}

		@Override
		public String convert(Map<String, Object> source) {
			if (source == null) {
				return null;
			}
			StringBuilder builder = new StringBuilder();
			builder.append(converters.get(0).convert(source));
			for (int index = 1; index < converters.size(); index++) {
				builder.append(separator);
				builder.append(converters.get(index).convert(source));
			}
			return builder.toString();
		}

	}

}
