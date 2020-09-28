package com.redislabs.riot.convert;

import lombok.Builder;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.Map;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

public interface KeyMaker extends Converter<Map<String, Object>, String> {

	String DEFAULT_SEPARATOR = ":";

	String EMPTY_STRING = "";

	static KeyMakerBuilder builder() {
		return new KeyMakerBuilder();
	}

	@Accessors(fluent = true)
	@SuppressWarnings("unchecked")
	public static class KeyMakerBuilder {
		@Setter
		private String separator = DEFAULT_SEPARATOR;
		@Setter
		private String prefix = EMPTY_STRING;
		private Converter<Map<String, Object>, Object>[] keyExtractors = new Converter[0];

		public KeyMakerBuilder extractors(Converter<Map<String, Object>, Object>... keyExtractors) {
			this.keyExtractors = keyExtractors;
			return this;
		}

		private String getPrefix() {
			if (prefix == null || prefix.isEmpty()) {
				return EMPTY_STRING;
			}
			return prefix + separator;
		}

		public KeyMaker build() {
			if (keyExtractors == null || keyExtractors.length == 0) {
				Assert.isTrue(prefix != null && !prefix.isEmpty(), "No keyspace nor key fields specified");
				return NoKeyMaker.builder().prefix(prefix).build();
			}
			if (keyExtractors.length == 1) {
				return SingleKeyMaker.builder().prefix(getPrefix()).keyExtractor(keyExtractors[0]).build();
			}
			return MultiKeyMaker.builder().prefix(getPrefix()).separator(separator).keyExtractors(keyExtractors)
					.build();
		}

	}

	@Builder
	public static class NoKeyMaker implements KeyMaker {

		@NonNull
		private final String prefix;

		@Override
		public String convert(Map<String, Object> source) {
			return prefix;
		}
	}

	@Builder
	public static class SingleKeyMaker<T> implements KeyMaker {

		@NonNull
		private final String prefix;
		@NonNull
		private final Converter<Map<String, Object>, Object> keyExtractor;

		@Override
		public String convert(Map<String, Object> source) {
			return prefix + keyExtractor.convert(source);
		}
	}

	@Builder
	public static class MultiKeyMaker implements KeyMaker {

		@NonNull
		private final String prefix;
		@NonNull
		@Builder.Default
		private final String separator = DEFAULT_SEPARATOR;
		@NonNull
		private final Converter<Map<String, Object>, Object>[] keyExtractors;

		@Override
		public String convert(Map<String, Object> source) {
			StringBuilder builder = new StringBuilder();
			builder.append(prefix);
			for (int index = 0; index < keyExtractors.length - 1; index++) {
				builder.append(keyExtractors[index].convert(source));
				builder.append(separator);
			}
			builder.append(keyExtractors[keyExtractors.length - 1].convert(source));
			return builder.toString();
		}

	}

}
